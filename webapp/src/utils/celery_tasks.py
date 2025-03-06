import time
from src.main.celery_app import celery_app
from src.data_scrapping.collectors_agg import CollectorsAggregator


@celery_app.task(name="my_collection_task", bind=False)
def collection_task(begin_date, end_date):
    try:
        collector = CollectorsAggregator(
            begin_date=begin_date,
            end_date=end_date,
            timeout=10,
        )
        collector.run()

        return {"status": "completed", "result": "Task Completed!"}
    except Exception as e:
        raise ValueError(f"Task failed: {str(e)}")
    

def revoke_task(task_id):
    celery_app.control.revoke(task_id, terminate=True, signal="SIGTERM")