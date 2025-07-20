import os
import zipfile
import logging
import pandas as pd
from io import StringIO

from src.main.celery_app import celery_app
from src.helpers.db_connector import DBConnector, DBManager
from src.helpers.enum import DBCOLUMNS, CeleryTasks, JobsKeys
from src.data_scrapping.collectors_agg import CollectorsAggregator


logger = logging.getLogger(__name__)
db_manager = DBManager()


@celery_app.task(name=CeleryTasks.collect, bind=False)
def collection_task(archive, begin_date, end_date):
    try:
        collector = CollectorsAggregator(
            archive,
            begin_date=begin_date,
            end_date=end_date,
            timeout=10,
        )
        collector.run()

        return {JobsKeys.STATUS: "completed", "result": "Task Completed!"}
    except Exception as e:
        raise ValueError(f"Task failed: {str(e)}")


@celery_app.task(name=CeleryTasks.download, bind=False)
def download_task(columns, filters, order):
    chunk_index = 1
    CHUNK_SIZE = 100_000
    zip_path = "/images/data.zip"
    if os.path.exists(zip_path):
        os.remove(zip_path)

    with zipfile.ZipFile(zip_path, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        last_seen = None
        while True:
            args = DBConnector.fetch_data_keyset(
                db_manager.engine,
                DBConnector.TABLE,
                last_seen_value=last_seen,
                limit=CHUNK_SIZE,
                filters=filters,
                columns=columns,
                desc_order=order,
            )

            if args is None or len(args) == 0:
                break
            last_seen = {
                DBCOLUMNS.date: args[-1][1],
                DBCOLUMNS.rowid: args[-1][0],
            }
            df = pd.DataFrame(args, columns=columns)
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False)
            zf.writestr(f"data_chunk_{chunk_index:03d}.csv", csv_buffer.getvalue())
            chunk_index += 1

    return zip_path


def revoke_task(task_id):
    celery_app.control.revoke(task_id, terminate=True, signal="SIGKILL")
