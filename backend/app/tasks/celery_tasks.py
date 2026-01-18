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
    from sqlalchemy import MetaData, Table, select, inspect, text, func, tuple_

    chunk_index = 1
    CHUNK_SIZE = 10_000
    MAX_ARTICLES = 100_000  # Total limit to prevent server RAM overload
    total_fetched = 0
    zip_path = f"{settings.IMAGES_PATH}/data.zip"

    if os.path.exists(zip_path):
        os.remove(zip_path)

    engine = db_manager.get_engine()
    inspector = inspect(engine)

    if "articles" not in inspector.get_table_names():
        return zip_path

    metadata = MetaData()
    table_ref = Table("articles", metadata, autoload_with=engine)

    columns_to_select = [DBCOLUMNS(c) if isinstance(c, str) else c for c in columns]

    with zipfile.ZipFile(zip_path, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        last_seen = None

        while total_fetched < MAX_ARTICLES:
            # Calculate remaining articles to fetch
            remaining = MAX_ARTICLES - total_fetched
            current_limit = min(CHUNK_SIZE, remaining)

            with engine.connect() as conn:
                with conn.begin():
                    conn.execute(
                        text("SET LOCAL hnsw.ef_search = :ef_val"),
                        {"ef_val": settings.HNSW_EF_SEARCH},
                    )
                    conn.execute(text("SET LOCAL hnsw.iterative_scan = relaxed_order"))

                    query = select(*[table_ref.c[c] for c in columns_to_select])

                    if last_seen:
                        last_date = last_seen.get(DBCOLUMNS.date) or last_seen.get(
                            "date"
                        )
                        last_rowid = last_seen.get(DBCOLUMNS.rowid) or last_seen.get(
                            "rowid"
                        )

                        if isinstance(last_date, str):
                            last_date = date.fromisoformat(last_date)

                        if order:
                            query = query.where(
                                tuple_(table_ref.c.date, table_ref.c.rowid)
                                < (last_date, last_rowid)
                            )
                        else:
                            query = query.where(
                                tuple_(table_ref.c.date, table_ref.c.rowid)
                                > (last_date, last_rowid)
                            )

                    if order:
                        query = query.order_by(
                            table_ref.c.date.desc(), table_ref.c.rowid.desc()
                        )
                    else:
                        query = query.order_by(
                            table_ref.c.date.asc(), table_ref.c.rowid.asc()
                        )

                    query = query.limit(current_limit)
                    result = conn.execute(query)
                    rows = result.fetchall()

            if not rows:
                break

            total_fetched += len(rows)

            last_seen = {
                DBCOLUMNS.date: (
                    rows[-1][1].isoformat()
                    if hasattr(rows[-1][1], "isoformat")
                    else rows[-1][1]
                ),
                DBCOLUMNS.rowid: rows[-1][0],
            }

            df = pd.DataFrame(rows, columns=columns_to_select)
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False)
            zf.writestr(f"data_chunk_{chunk_index:03d}.csv", csv_buffer.getvalue())
            chunk_index += 1

            if total_fetched >= MAX_ARTICLES:
                logger.info(f"Download limit reached: {MAX_ARTICLES} articles")
                break

    logger.info(
        f"Download complete: {total_fetched} articles in {chunk_index - 1} chunks"
    )
    return zip_path


def revoke_task(task_id):
    celery_app.control.revoke(task_id, terminate=True, signal="SIGKILL")
