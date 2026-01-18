#!/usr/bin/env python3
"""
Migration script to upload local images to S3.

Features:
- Parallel uploads using ThreadPoolExecutor
- Progress tracking with checkpoint file
- Resume capability if interrupted
- Configurable batch size and worker count
- Progress bar with tqdm
- Error logging for failed uploads

Usage:
    python migrate_images_to_s3.py --source /path/to/images --workers 50

Environment variables required:
    S3_ENDPOINT_URL, S3_ACCESS_KEY, S3_SECRET_KEY, S3_BUCKET_NAME, S3_REGION
"""

import argparse
import json
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from tqdm import tqdm

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)
load_dotenv()

class S3Migrator:
    """Handles migration of local images to S3."""

    def __init__(
        self,
        source_dir: str,
        endpoint_url: str,
        access_key: str,
        secret_key: str,
        bucket_name: str,
        region: str,
        workers: int = 50,
        checkpoint_file: str = "migration_checkpoint.json",
        error_log_file: str = "migration_errors.log",
        checkpoint_interval: int = 1000,
    ):
        self.source_dir = Path(source_dir)
        self.bucket_name = bucket_name
        self.workers = workers
        self.checkpoint_file = Path(checkpoint_file)
        self.error_log_file = Path(error_log_file)
        self.checkpoint_interval = checkpoint_interval

        # Initialize S3 client with increased connection pool for parallel uploads
        s3_config = Config(
            max_pool_connections=workers + 10,
            retries={"max_attempts": 3, "mode": "adaptive"},
        )
        self.s3_client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region,
            config=s3_config,
        )

        # Load checkpoint if exists
        self.uploaded_files: set[str] = set()
        self._load_checkpoint()

        # Statistics
        self.stats = {
            "total_files": 0,
            "uploaded": 0,
            "skipped": 0,
            "failed": 0,
            "start_time": None,
        }

    def _load_checkpoint(self) -> None:
        """Load checkpoint file to resume from previous run."""
        if self.checkpoint_file.exists():
            try:
                with open(self.checkpoint_file) as f:
                    data = json.load(f)
                    self.uploaded_files = set(data.get("uploaded_files", []))
                    logger.info(f"Loaded checkpoint with {len(self.uploaded_files)} already uploaded files")
            except (json.JSONDecodeError, IOError) as e:
                logger.warning(f"Could not load checkpoint file: {e}")

    def _save_checkpoint(self) -> None:
        """Save checkpoint to file."""
        try:
            with open(self.checkpoint_file, "w") as f:
                json.dump(
                    {
                        "uploaded_files": list(self.uploaded_files),
                        "last_updated": datetime.now().isoformat(),
                        "stats": self.stats,
                    },
                    f,
                )
        except IOError as e:
            logger.error(f"Failed to save checkpoint: {e}")

    def _log_error(self, file_path: str, error: str) -> None:
        """Log error to error log file."""
        try:
            with open(self.error_log_file, "a") as f:
                f.write(f"{datetime.now().isoformat()} | {file_path} | {error}\n")
        except IOError as e:
            logger.error(f"Failed to write to error log: {e}")

    def _get_s3_key(self, file_path: Path) -> str:
        """Convert local file path to S3 key.

        Preserves the directory structure: year/month/hash.webp
        """
        relative_path = file_path.relative_to(self.source_dir)
        return str(relative_path).replace("\\", "/")

    def _upload_file(self, file_path: Path) -> tuple[str, bool, str]:
        """Upload a single file to S3.

        Returns:
            Tuple of (file_path_str, success, error_message)
        """
        file_path_str = str(file_path)
        s3_key = self._get_s3_key(file_path)

        # Skip if already uploaded
        if s3_key in self.uploaded_files:
            return file_path_str, True, "skipped"

        try:
            # Determine content type
            content_type = "image/webp"
            if file_path.suffix.lower() in [".jpg", ".jpeg"]:
                content_type = "image/jpeg"
            elif file_path.suffix.lower() == ".png":
                content_type = "image/png"

            # Upload file
            self.s3_client.upload_file(
                str(file_path),
                self.bucket_name,
                s3_key,
                ExtraArgs={"ContentType": content_type},
            )

            return file_path_str, True, ""

        except ClientError as e:
            error_msg = str(e)
            return file_path_str, False, error_msg
        except Exception as e:
            error_msg = f"Unexpected error: {e}"
            return file_path_str, False, error_msg

    def scan_files(self) -> list[Path]:
        """Scan source directory for image files."""
        logger.info(f"Scanning {self.source_dir} for images...")

        image_extensions = {".webp", ".jpg", ".jpeg", ".png", ".gif"}
        files = []

        for file_path in self.source_dir.rglob("*"):
            if file_path.is_file() and file_path.suffix.lower() in image_extensions:
                files.append(file_path)

        logger.info(f"Found {len(files)} image files")
        return files

    def migrate(self, dry_run: bool = False) -> None:
        """Run the migration."""
        self.stats["start_time"] = datetime.now().isoformat()

        # Scan for files
        files = self.scan_files()
        self.stats["total_files"] = len(files)

        if dry_run:
            logger.info("Dry run mode - no files will be uploaded")
            for f in files[:10]:
                s3_key = self._get_s3_key(f)
                status = "SKIP" if s3_key in self.uploaded_files else "UPLOAD"
                logger.info(f"[{status}] {f} -> {s3_key}")
            if len(files) > 10:
                logger.info(f"... and {len(files) - 10} more files")
            return

        # Filter out already uploaded files
        files_to_upload = [f for f in files if self._get_s3_key(f) not in self.uploaded_files]
        self.stats["skipped"] = len(files) - len(files_to_upload)

        logger.info(
            f"Uploading {len(files_to_upload)} files "
            f"({self.stats['skipped']} already uploaded)"
        )

        if not files_to_upload:
            logger.info("No new files to upload")
            return

        # Upload files in parallel
        uploaded_count = 0
        with ThreadPoolExecutor(max_workers=self.workers) as executor:
            futures = {
                executor.submit(self._upload_file, f): f for f in files_to_upload
            }

            with tqdm(total=len(files_to_upload), desc="Uploading") as pbar:
                for future in as_completed(futures):
                    file_path_str, success, error = future.result()
                    s3_key = self._get_s3_key(Path(file_path_str))

                    if success:
                        if error != "skipped":
                            self.uploaded_files.add(s3_key)
                            self.stats["uploaded"] += 1
                            uploaded_count += 1
                    else:
                        self.stats["failed"] += 1
                        self._log_error(file_path_str, error)
                        logger.warning(f"Failed to upload {file_path_str}: {error}")

                    pbar.update(1)

                    # Save checkpoint periodically
                    if uploaded_count > 0 and uploaded_count % self.checkpoint_interval == 0:
                        self._save_checkpoint()
                        logger.info(f"Checkpoint saved ({uploaded_count} files uploaded)")

        # Final checkpoint save
        self._save_checkpoint()

        # Print summary
        logger.info("Migration complete!")
        logger.info(f"  Total files: {self.stats['total_files']}")
        logger.info(f"  Uploaded: {self.stats['uploaded']}")
        logger.info(f"  Skipped (already uploaded): {self.stats['skipped']}")
        logger.info(f"  Failed: {self.stats['failed']}")

        if self.stats["failed"] > 0:
            logger.warning(f"Check {self.error_log_file} for failed uploads")

    def verify(self, sample_size: int = 100) -> None:
        """Verify uploaded files by checking a sample."""
        files = self.scan_files()

        if not files:
            logger.info("No files to verify")
            return

        # Sample files to verify
        import random
        sample = random.sample(files, min(sample_size, len(files)))

        logger.info(f"Verifying {len(sample)} files...")

        verified = 0
        missing = 0

        for file_path in tqdm(sample, desc="Verifying"):
            s3_key = self._get_s3_key(file_path)
            try:
                self.s3_client.head_object(Bucket=self.bucket_name, Key=s3_key)
                verified += 1
            except ClientError as e:
                if e.response["Error"]["Code"] == "404":
                    missing += 1
                    logger.warning(f"Missing: {s3_key}")
                else:
                    logger.error(f"Error checking {s3_key}: {e}")

        logger.info(f"Verification complete: {verified} verified, {missing} missing")


def main():
    parser = argparse.ArgumentParser(
        description="Migrate local images to S3 storage"
    )
    parser.add_argument(
        "--source",
        type=str,
        default=os.path.expanduser("~/archives_collection_images"),
        help="Source directory containing images",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=50,
        help="Number of parallel upload workers",
    )
    parser.add_argument(
        "--checkpoint-file",
        type=str,
        default="migration_checkpoint.json",
        help="Checkpoint file for resume capability",
    )
    parser.add_argument(
        "--checkpoint-interval",
        type=int,
        default=1000,
        help="Save checkpoint every N uploads",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Scan files without uploading",
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="Verify uploaded files instead of uploading",
    )
    parser.add_argument(
        "--verify-sample",
        type=int,
        default=100,
        help="Number of files to verify",
    )

    args = parser.parse_args()

    # Get S3 configuration from environment
    endpoint_url = os.environ.get("S3_ENDPOINT_URL")
    access_key = os.environ.get("S3_ACCESS_KEY")
    secret_key = os.environ.get("S3_SECRET_KEY")
    bucket_name = os.environ.get("S3_BUCKET_NAME", "archives-images")
    region = os.environ.get("S3_REGION", "gra")

    if not all([endpoint_url, access_key, secret_key]):
        logger.error(
            "Missing required environment variables: "
            "S3_ENDPOINT_URL, S3_ACCESS_KEY, S3_SECRET_KEY"
        )
        sys.exit(1)

    if not Path(args.source).exists():
        logger.error(f"Source directory does not exist: {args.source}")
        sys.exit(1)

    migrator = S3Migrator(
        source_dir=args.source,
        endpoint_url=endpoint_url,
        access_key=access_key,
        secret_key=secret_key,
        bucket_name=bucket_name,
        region=region,
        workers=args.workers,
        checkpoint_file=args.checkpoint_file,
        checkpoint_interval=args.checkpoint_interval,
    )

    if args.verify:
        migrator.verify(sample_size=args.verify_sample)
    else:
        migrator.migrate(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
