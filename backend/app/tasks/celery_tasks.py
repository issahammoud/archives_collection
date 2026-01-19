import os
import zipfile
import logging
import pandas as pd
from io import StringIO
from datetime import date

from app.core.celery_app import celery_app
from app.core.config import settings
from app.core.enums import DBCOLUMNS, CeleryTasks
from app.core.sync_db import SyncDBManager

logger = logging.getLogger(__name__)
db_manager = SyncDBManager()


@celery_app.task(name=CeleryTasks.collect, bind=False)
def collection_task(archive, begin_date, end_date):
    # Import here to avoid circular imports at module load time
    # Import collectors first to register them with the Registry
    import app.data_scrapping.collectors  # noqa: F401
    from app.data_scrapping.collectors_agg import CollectorsAggregator

    try:
        if isinstance(begin_date, str):
            begin_date = date.fromisoformat(begin_date)
        if isinstance(end_date, str):
            end_date = date.fromisoformat(end_date)

        collector = CollectorsAggregator(
            archive,
            begin_date=begin_date,
            end_date=end_date,
            timeout=10,
        )
        collector.run()

        return {"status": "completed", "result": "Task Completed!"}
    except Exception as e:
        logger.error(f"Collection task failed: {str(e)}")
        raise ValueError(f"Task failed: {str(e)}")


@celery_app.task(name=CeleryTasks.download, bind=False)
def download_task(columns, filters, order):
    import tempfile
    import redis
    from sqlalchemy import MetaData, Table, select, inspect
    from app.core.s3_client import sync_s3_client

    MAX_ARTICLES = 500  # Limit to 500 rows for download with images
    REDIS_KEY = "download:data.zip"

    # Connect to Redis
    redis_client = redis.from_url(settings.REDIS_URL)

    engine = db_manager.get_engine()
    inspector = inspect(engine)

    if "articles" not in inspector.get_table_names():
        # Store empty zip in Redis
        redis_client.setex(REDIS_KEY, 3600, b"")
        return "done"

    metadata = MetaData()
    table_ref = Table("articles", metadata, autoload_with=engine)

    columns_to_select = [DBCOLUMNS(c) if isinstance(c, str) else c for c in columns]

    # Fetch articles (up to MAX_ARTICLES)
    logger.info("Fetching articles from database...")
    with engine.connect() as conn:
        with conn.begin():
            query = select(*[table_ref.c[c] for c in columns_to_select])

            # Apply filters using OPERATORS enum values
            from app.core.enums import OPERATORS
            from sqlalchemy import func
            for col_name, conditions in filters.items():
                col = table_ref.c[col_name]
                for op, value in conditions:
                    op_str = op.value if hasattr(op, 'value') else str(op)
                    if op_str == "ge":
                        query = query.where(col >= value)
                    elif op_str == "le":
                        query = query.where(col <= value)
                    elif op_str == "like":
                        query = query.where(func.upper(col).like(f"%{value.upper()}%"))
                    elif op_str == "in":
                        query = query.where(col.in_(value))
                    elif op_str == "text_search":
                        ts_query = func.websearch_to_tsquery("french", func.unaccent(value))
                        query = query.where(col.op("@@")(ts_query))
                    elif op_str == "notnull":
                        query = query.where(col.isnot(None))
                        query = query.where(col != "")

            if order:
                query = query.order_by(
                    table_ref.c.date.desc(), table_ref.c.rowid.desc()
                )
            else:
                query = query.order_by(
                    table_ref.c.date.asc(), table_ref.c.rowid.asc()
                )

            query = query.limit(MAX_ARTICLES)
            result = conn.execute(query)
            rows = result.fetchall()

    logger.info(f"Fetched {len(rows)} articles from database")

    if not rows:
        # Store empty zip in Redis
        from io import BytesIO
        buffer = BytesIO()
        with zipfile.ZipFile(buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("data.csv", "")
        redis_client.setex(REDIS_KEY, 3600, buffer.getvalue())
        return "done"

    # Create DataFrame
    df = pd.DataFrame(rows, columns=columns_to_select)

    # Get image column index if it exists
    image_col = DBCOLUMNS.image if DBCOLUMNS.image in columns_to_select else None

    # Create ZIP in memory
    from io import BytesIO
    buffer = BytesIO()

    logger.info(f"Starting ZIP creation with {len(df)} articles")

    with zipfile.ZipFile(buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        images_downloaded = 0
        images_failed = 0

        # Process images and update paths
        if image_col and image_col in df.columns:
            new_image_paths = []
            total_images = len(df)
            for idx, row in df.iterrows():
                s3_key = row[image_col]
                # Strip /images/ prefix if present
                if s3_key and isinstance(s3_key, str) and s3_key.startswith("/images/"):
                    s3_key = s3_key.replace("/images/", "", 1)
                if s3_key and pd.notna(s3_key):
                    # Download image from S3
                    image_bytes = sync_s3_client.download_image(s3_key)
                    if image_bytes:
                        # Add image to ZIP in images/ folder
                        image_filename = os.path.basename(s3_key)
                        zf.writestr(f"images/{image_filename}", image_bytes)
                        new_image_paths.append(f"images/{image_filename}")
                        images_downloaded += 1
                    else:
                        new_image_paths.append("")
                        images_failed += 1
                else:
                    new_image_paths.append("")

                # Log progress every 100 images
                processed = images_downloaded + images_failed
                if processed % 100 == 0:
                    logger.info(f"Download progress: {processed}/{total_images} images processed")

            # Update image column with new paths
            df[image_col] = new_image_paths

        # Write CSV to ZIP
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        zf.writestr("data.csv", csv_buffer.getvalue())

    # Store zip in Redis (expires in 1 hour)
    redis_client.setex(REDIS_KEY, 3600, buffer.getvalue())

    logger.info(
        f"Download complete: {len(rows)} articles, {images_downloaded} images downloaded, {images_failed} failed"
    )
    return "done"


def revoke_task(task_id):
    celery_app.control.revoke(task_id, terminate=True, signal="SIGKILL")
