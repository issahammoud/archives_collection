#!/usr/bin/env python3
"""
Sync checkpoint file with existing S3 objects.
Lists all objects in S3 bucket and updates the checkpoint file.
"""

import json
import os
from datetime import datetime
from pathlib import Path

import boto3
from botocore.config import Config
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()

def main():
    endpoint_url = os.environ.get("S3_ENDPOINT_URL")
    access_key = os.environ.get("S3_ACCESS_KEY")
    secret_key = os.environ.get("S3_SECRET_KEY")
    bucket_name = os.environ.get("S3_BUCKET_NAME", "archives-images")
    region = os.environ.get("S3_REGION", "gra")

    if not all([endpoint_url, access_key, secret_key]):
        print("Missing S3 environment variables")
        return

    s3_config = Config(max_pool_connections=10)
    s3_client = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
        config=s3_config,
    )

    print("Listing all objects in S3 bucket...")
    existing_keys = set()

    paginator = s3_client.get_paginator('list_objects_v2')

    for page in tqdm(paginator.paginate(Bucket=bucket_name), desc="Fetching pages"):
        if 'Contents' in page:
            for obj in page['Contents']:
                existing_keys.add(obj['Key'])

    print(f"\nFound {len(existing_keys)} objects in S3")

    # Save to checkpoint
    checkpoint_file = Path("migration_checkpoint.json")
    checkpoint_data = {
        "uploaded_files": list(existing_keys),
        "last_updated": datetime.now().isoformat(),
        "synced_from_s3": True,
    }

    with open(checkpoint_file, 'w') as f:
        json.dump(checkpoint_data, f)

    print(f"Checkpoint updated with {len(existing_keys)} files")

if __name__ == "__main__":
    main()
