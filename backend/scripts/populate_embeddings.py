#!/usr/bin/env python3
"""
Populate article_embeddings table with embeddings from Jina v4 model.

Features:
- Batch processing with configurable batch size
- Progress tracking with checkpoint file
- Resume capability if interrupted
- Progress bar with tqdm
- Error logging for failed batches

Usage:
    python populate_embeddings.py --batch-size 32

Environment variables required:
    POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB, DATABASE_HOST, DATABASE_PORT
"""

import argparse
import json
import logging
import sys
from datetime import datetime
from pathlib import Path

# Add backend root to path for imports (script is at /backend/scripts/)
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
from tqdm import tqdm
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from app.core.config import settings
from app.core.utils import JinaEmbedder

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)
load_dotenv()

EMBEDDING_DIM = 256
MAX_TEXT_CHARS = 1500  # ~400 tokens, model max is 512 (French text has ~3.5 chars/token)


class EmbeddingPopulator:
    """Handles populating the article_embeddings table."""

    def __init__(
        self,
        batch_size: int = 8,
        checkpoint_file: str = "/tmp/embedding_checkpoint.json",
        error_log_file: str = "/tmp/embedding_errors.log",
        checkpoint_interval: int = 50,
        embedding_url: str = "http://embedding:8000/v1",
    ):
        self.batch_size = batch_size
        self.checkpoint_file = Path(checkpoint_file)
        self.error_log_file = Path(error_log_file)
        self.checkpoint_interval = checkpoint_interval

        # Initialize database connection
        self.engine = create_engine(
            settings.sync_database_url,
            pool_size=2,
            max_overflow=0,
            pool_pre_ping=True,
        )
        self.Session = sessionmaker(bind=self.engine)

        # Initialize embedder
        self.embedder = JinaEmbedder(base_url=embedding_url)

        # Load checkpoint if exists
        self.last_processed_rowid: int = 0
        self._load_checkpoint()

        # Statistics
        self.stats = {
            "total_articles": 0,
            "embedded": 0,
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
                    self.last_processed_rowid = data.get("last_processed_rowid", 0)
                    logger.info(f"Loaded checkpoint: resuming from rowid > {self.last_processed_rowid}")
            except (json.JSONDecodeError, IOError) as e:
                logger.warning(f"Could not load checkpoint file: {e}")

    def _save_checkpoint(self, last_rowid: int) -> None:
        """Save checkpoint to file."""
        try:
            with open(self.checkpoint_file, "w") as f:
                json.dump(
                    {
                        "last_processed_rowid": last_rowid,
                        "last_updated": datetime.now().isoformat(),
                        "stats": self.stats,
                    },
                    f,
                )
            self.last_processed_rowid = last_rowid
        except IOError as e:
            logger.error(f"Failed to save checkpoint: {e}")

    def _log_error(self, rowids: list[int], error: str) -> None:
        """Log error to error log file."""
        try:
            with open(self.error_log_file, "a") as f:
                f.write(f"{datetime.now().isoformat()} - rowids {rowids}: {error}\n")
        except IOError:
            pass

    def _prepare_text(self, title: str | None, content: str | None, tag: str | None) -> str | None:
        """Prepare text for embedding from title, content, and tag. Returns None if all empty."""
        parts = []
        if title:
            parts.append(f"title: {title}")
        if content:
            parts.append(f"content: {content}")
        if tag:
            parts.append(f"topic: {tag}")
        if not parts:
            return None
        text = "\n".join(parts)
        # Truncate to avoid exceeding model's token limit (512 tokens)
        return text[:MAX_TEXT_CHARS] if len(text) > MAX_TEXT_CHARS else text

    def get_pending_articles_count(self) -> int:
        """Get count of articles without embeddings."""
        with self.Session() as session:
            result = session.execute(text("""
                SELECT COUNT(*)
                FROM articles a
                LEFT JOIN article_embeddings ae ON a.rowid = ae.rowid
                WHERE ae.rowid IS NULL AND a.rowid > :last_rowid
            """), {"last_rowid": self.last_processed_rowid})
            return result.scalar()

    def get_batch(self, offset: int = 0) -> list[dict]:
        """Fetch a batch of articles that need embeddings."""
        with self.Session() as session:
            result = session.execute(text("""
                SELECT a.rowid, a.title, a.content, a.tag
                FROM articles a
                LEFT JOIN article_embeddings ae ON a.rowid = ae.rowid
                WHERE ae.rowid IS NULL AND a.rowid > :last_rowid
                ORDER BY a.rowid
                LIMIT :batch_size
            """), {"last_rowid": self.last_processed_rowid, "batch_size": self.batch_size})

            return [
                {"rowid": row.rowid, "title": row.title, "content": row.content, "tag": row.tag}
                for row in result
            ]

    def embed_batch(self, articles: list[dict]) -> list[tuple[int, list[float] | None]] | None:
        """Generate embeddings for a batch of articles. Returns None on error."""
        # Separate articles with text from those without
        articles_with_text = []
        results = []

        for a in articles:
            text = self._prepare_text(a["title"], a["content"], a["tag"])
            if text:
                articles_with_text.append((a, text))
            else:
                # Empty articles get NULL embedding
                results.append((a["rowid"], None))
                self.stats["skipped"] += 1

        if not articles_with_text:
            return results

        texts = [text for _, text in articles_with_text]

        try:
            embeddings = self.embedder.embed(texts)
            for (a, _), emb in zip(articles_with_text, embeddings):
                results.append((a["rowid"], emb))
            return results
        except Exception as e:
            # Batch failed - try one by one to identify problematic articles
            logger.warning(f"Batch failed, retrying individually: {e}")
            for a, text in articles_with_text:
                try:
                    emb = self.embedder.embed(text)  # Single text returns single embedding
                    results.append((a["rowid"], emb))
                except Exception:
                    # This specific article failed - store NULL
                    logger.warning(f"Article {a['rowid']} failed to embed, storing NULL")
                    results.append((a["rowid"], None))
                    self.stats["skipped"] += 1
            return results

    def insert_embeddings(self, embeddings: list[tuple[int, list[float] | None]]) -> bool:
        """Insert embeddings into the database."""
        with self.Session() as session:
            try:
                for rowid, embedding in embeddings:
                    embedding_value = str(embedding) if embedding is not None else None
                    session.execute(
                        text("""
                            INSERT INTO article_embeddings (rowid, embedding)
                            VALUES (:rowid, :embedding)
                            ON CONFLICT (rowid) DO UPDATE SET embedding = :embedding
                        """),
                        {"rowid": rowid, "embedding": embedding_value}
                    )
                session.commit()
                return True
            except Exception as e:
                session.rollback()
                logger.error(f"Error inserting embeddings: {e}")
                return False

    def populate(self, dry_run: bool = False) -> None:
        """Main method to populate embeddings."""
        self.stats["start_time"] = datetime.now().isoformat()

        # Get total count
        pending_count = self.get_pending_articles_count()
        self.stats["total_articles"] = pending_count
        logger.info(f"Found {pending_count} articles to embed")

        if pending_count == 0:
            logger.info("No articles to process")
            return

        if dry_run:
            logger.info("Dry run mode - no changes will be made")
            return

        # Process in batches with progress bar
        batches_processed = 0
        with tqdm(total=pending_count, desc="Embedding articles", unit="article") as pbar:
            while True:
                batch = self.get_batch()
                if not batch:
                    break

                # Generate embeddings
                embeddings = self.embed_batch(batch)
                if embeddings is None:
                    rowids = [a["rowid"] for a in batch]
                    self._log_error(rowids, "Failed to generate embeddings")
                    self.stats["failed"] += len(batch)
                    # Update checkpoint to skip failed batch
                    self._save_checkpoint(max(rowids))
                    pbar.update(len(batch))
                    continue

                # Insert into database
                if self.insert_embeddings(embeddings):
                    # Count only non-null embeddings (skipped already counted in embed_batch)
                    self.stats["embedded"] += sum(1 for _, emb in embeddings if emb is not None)
                    last_rowid = max(a["rowid"] for a in batch)

                    batches_processed += 1
                    if batches_processed % self.checkpoint_interval == 0:
                        self._save_checkpoint(last_rowid)
                    else:
                        self.last_processed_rowid = last_rowid
                else:
                    rowids = [a["rowid"] for a in batch]
                    self._log_error(rowids, "Failed to insert embeddings")
                    self.stats["failed"] += len(batch)
                    self._save_checkpoint(max(rowids))

                pbar.update(len(batch))

        # Save final checkpoint
        self._save_checkpoint(self.last_processed_rowid)

        # Print summary
        elapsed = datetime.now() - datetime.fromisoformat(self.stats["start_time"])
        logger.info(f"\nPopulation complete!")
        logger.info(f"  Total articles: {self.stats['total_articles']}")
        logger.info(f"  Embedded: {self.stats['embedded']}")
        logger.info(f"  Skipped (empty): {self.stats['skipped']}")
        logger.info(f"  Failed: {self.stats['failed']}")
        logger.info(f"  Time elapsed: {elapsed}")

    def verify(self, sample_size: int = 100) -> None:
        """Verify embeddings by checking a sample."""
        with self.Session() as session:
            # Check total counts
            result = session.execute(text("SELECT COUNT(*) FROM articles"))
            total_articles = result.scalar()

            result = session.execute(text("SELECT COUNT(*) FROM article_embeddings"))
            total_embeddings = result.scalar()

            result = session.execute(text("SELECT COUNT(*) FROM article_embeddings WHERE embedding IS NULL"))
            null_embeddings = result.scalar()

            logger.info(f"Total articles: {total_articles}")
            logger.info(f"Total embedding rows: {total_embeddings}")
            logger.info(f"  With embedding: {total_embeddings - null_embeddings}")
            logger.info(f"  NULL (empty text): {null_embeddings}")
            logger.info(f"Coverage: {total_embeddings / total_articles * 100:.1f}%")

            # Check embedding dimensions
            result = session.execute(text("""
                SELECT rowid, vector_dims(embedding) as dims
                FROM article_embeddings
                LIMIT :sample_size
            """), {"sample_size": sample_size})

            dims_check = list(result)
            if dims_check:
                incorrect = [r for r in dims_check if r.dims != EMBEDDING_DIM]
                if incorrect:
                    logger.warning(f"Found {len(incorrect)} embeddings with incorrect dimensions!")
                else:
                    logger.info(f"All {len(dims_check)} sampled embeddings have correct dimension ({EMBEDDING_DIM})")


def main():
    parser = argparse.ArgumentParser(description="Populate article embeddings")
    parser.add_argument(
        "--batch-size",
        type=int,
        default=32,
        help="Batch size for embedding (default: 8, conservative for 8GB GPU)",
    )
    parser.add_argument(
        "--checkpoint-file",
        type=str,
        default="/tmp/embedding_checkpoint.json",
        help="Checkpoint file path",
    )
    parser.add_argument(
        "--embedding-url",
        type=str,
        default="http://embedding:8000/v1",
        help="Embedding service URL",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without making changes",
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="Verify existing embeddings",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Reset checkpoint and start from beginning",
    )

    args = parser.parse_args()

    if args.reset:
        checkpoint_path = Path(args.checkpoint_file)
        if checkpoint_path.exists():
            checkpoint_path.unlink()
            logger.info("Checkpoint reset")

    populator = EmbeddingPopulator(
        batch_size=args.batch_size,
        checkpoint_file=args.checkpoint_file,
        embedding_url=args.embedding_url,
    )

    if args.verify:
        populator.verify()
    else:
        populator.populate(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
