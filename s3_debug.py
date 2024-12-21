# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "click",
#     "boto3",
#     "urllib3",
#     "rich",
# ]
# ///

import re
import sys
from urllib.parse import urlparse
import click
import boto3
from botocore.exceptions import ClientError
from rich.console import Console
from rich.table import Table

console = Console()

def extract_bucket_and_key(url):
    """Extract bucket name and key from either URL format."""
    # Handle both URL patterns
    if '.s3.' in url:
        # Format: https://bucket-name.s3.region.amazonaws.com/key
        bucket = url.split('.s3.')[0].split('/')[-1]
        key = '/'.join(url.split('.amazonaws.com/')[-1].split('/'))
    else:
        # Format: https://s3.amazonaws.com/bucket-name/key
        parts = urlparse(url).path.lstrip('/').split('/', 1)
        bucket = parts[0]
        key = parts[1] if len(parts) > 1 else ''
    
    return bucket, key

def check_bucket_exists(s3_client, bucket):
    """Check if bucket exists and we have access to it."""
    try:
        s3_client.head_bucket(Bucket=bucket)
        return True
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        if error_code == '404':
            console.print(f"[red]Bucket {bucket} does not exist[/red]")
        elif error_code == '403':
            console.print(f"[yellow]Access denied to bucket {bucket}[/yellow]")
        else:
            console.print(f"[red]Error checking bucket: {str(e)}[/red]")
        return False

def check_object_metadata(s3_client, bucket, key):
    """Try to get object metadata."""
    try:
        metadata = s3_client.head_object(Bucket=bucket, Key=key)
        console.print("[green]Object metadata found:[/green]")
        for k, v in metadata.items():
            if k != 'ResponseMetadata':
                console.print(f"{k}: {v}")
        return True
    except ClientError as e:
        console.print(f"[red]Could not get object metadata: {str(e)}[/red]")
        return False

def list_similar_objects(s3_client, bucket, key):
    """List objects with similar prefixes."""
    prefix = '/'.join(key.split('/')[:-1])
    try:
        response = s3_client.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix,
            MaxKeys=10
        )
        if 'Contents' in response:
            console.print("\n[green]Found similar objects:[/green]")
            table = Table(show_header=True)
            table.add_column("Key")
            table.add_column("Size")
            table.add_column("Last Modified")
            
            for obj in response['Contents']:
                table.add_row(
                    obj['Key'],
                    str(obj['Size']),
                    str(obj['LastModified'])
                )
            console.print(table)
        else:
            console.print(f"[yellow]No objects found with prefix: {prefix}[/yellow]")
    except ClientError as e:
        console.print(f"[red]Error listing objects: {str(e)}[/red]")

def check_bucket_policy(s3_client, bucket):
    """Try to retrieve and analyze bucket policy."""
    try:
        policy = s3_client.get_bucket_policy(Bucket=bucket)
        console.print("[green]Bucket policy found:[/green]")
        console.print(policy['Policy'])
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchBucketPolicy':
            console.print("[yellow]No bucket policy found[/yellow]")
        else:
            console.print(f"[red]Error getting bucket policy: {str(e)}[/red]")

def check_bucket_location(s3_client, bucket):
    """Get bucket location."""
    try:
        location = s3_client.get_bucket_location(Bucket=bucket)
        region = location.get('LocationConstraint') or 'us-east-1'
        console.print(f"[green]Bucket region: {region}[/green]")
        return region
    except ClientError as e:
        console.print(f"[red]Error getting bucket location: {str(e)}[/red]")
        return None

@click.command()
@click.argument('url')
@click.option('--profile', help='AWS profile to use')
@click.option('--region', help='AWS region to use')
def debug_s3_access(url, profile, region):
    """Debug S3 access issues for a given URL."""
    bucket, key = extract_bucket_and_key(url)
    console.print(f"[bold]Analyzing URL: {url}[/bold]")
    console.print(f"Bucket: {bucket}")
    console.print(f"Key: {key}\n")

    session = boto3.Session(profile_name=profile)
    s3_client = session.client('s3', region_name=region)

    # Check bucket exists and is accessible
    if not check_bucket_exists(s3_client, bucket):
        return

    # Get bucket location and verify region
    actual_region = check_bucket_location(s3_client, bucket)
    if actual_region and region and actual_region != region:
        console.print(f"[yellow]Warning: Specified region {region} differs from bucket region {actual_region}[/yellow]")
        s3_client = session.client('s3', region_name=actual_region)

    # Check object metadata
    check_object_metadata(s3_client, bucket, key)

    # List similar objects
    list_similar_objects(s3_client, bucket, key)

    # Check bucket policy
    check_bucket_policy(s3_client, bucket)

    # Additional bucket checks
    try:
        acl = s3_client.get_bucket_acl(Bucket=bucket)
        console.print("\n[green]Bucket ACL:[/green]")
        for grant in acl['Grants']:
            console.print(f"- Grantee: {grant['Grantee'].get('DisplayName', 'Unknown')}")
            console.print(f"  Permission: {grant['Permission']}")
    except ClientError as e:
        console.print(f"[red]Error getting bucket ACL: {str(e)}[/red]")

if __name__ == '__main__':
    debug_s3_access()