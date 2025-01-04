import orjson
import asyncio
import aiohttp
import numpy as np
from tqdm import tqdm
from src.utils.logging import logging
from src.helpers.enum import DBCOLUMNS
from src.helpers.db_connector import DBConnector


logger = logging.getLogger(__name__)


async def insert_embeddings(batch, embedding_url, session):
    texts = [item["text"] for item in batch]
    ids = [item["id"] for item in batch]

    try:
        async with session.post(embedding_url, json={"text": texts}) as response:
            response.raise_for_status()
            embeddings = np.array(orjson.loads(await response.read())["embeddings"])

            values = [
                {"id": _id, "embedding": emb} for _id, emb in zip(ids, embeddings)
            ]
            DBConnector.update_embedding(
                DBConnector.get_engine(), DBConnector.TABLE, values
            )

    except Exception as e:
        logger.error(f"Error processing batch: {e}")


async def main_loop(fetch_batches_function, embedding_url, batch_size, total_rows):

    async with aiohttp.ClientSession() as session:
        pbar = tqdm(total=total_rows, desc="Embedding Progress", unit="row")

        while True:
            batch = fetch_batches_function(batch_size)
            if len(batch) == 0:
                break

            await insert_embeddings(batch, embedding_url, session)

            pbar.update(len(batch))

        pbar.close()


class FetchBatchesFunction:
    def __init__(self):
        self._last_seen = None

    def __call__(self, batch_size):
        batch = []
        fetched_data = DBConnector.fetch_data_keyset(
            DBConnector.get_engine(),
            DBConnector.TABLE,
            last_seen_value=self._last_seen,
            columns=[
                DBCOLUMNS.rowid,
                DBCOLUMNS.date,
                DBCOLUMNS.title,
                DBCOLUMNS.content,
                DBCOLUMNS.tag,
            ],
            limit=batch_size,
            filters={DBCOLUMNS.embedding: [("isnull", None)]},
        )

        self._last_seen = {
            DBCOLUMNS.rowid: fetched_data[-1][0],
            DBCOLUMNS.date: fetched_data[-1][1],
        }

        for el in fetched_data:
            text = el[2] + "\n" if el[2] else ""
            text += el[3] + "\n" if el[3] else ""
            text += el[4] if el[4] else ""

            batch.append({"id": el[0], "text": text})

        return batch


if __name__ == "__main__":
    total_rows = DBConnector.get_total_count(
        DBConnector.get_engine(),
        DBConnector.TABLE,
        filters={DBCOLUMNS.embedding: [("isnull", None)]},
    )
    batch_size = 128
    embedding_url = "http://embedding:8000/embed"

    fetch_batches_function = FetchBatchesFunction()

    asyncio.run(
        main_loop(fetch_batches_function, embedding_url, batch_size, total_rows)
    )
