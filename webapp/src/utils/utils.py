import re
import os
import base64
import orjson
import hashlib
import logging
import requests
import itertools
import numpy as np
from functools import wraps
from wand.image import Image as WandImage
from sqlalchemy import inspect, MetaData, Table, text, Select

from src.helpers.enum import DBCOLUMNS

logger = logging.getLogger(__name__)


def alternate_elements(list_of_list):
    pad_token = ("to_delete", "to_delete")
    padded = np.array(
        list(zip(*itertools.zip_longest(*list_of_list, fillvalue=pad_token)))
    )
    col_1 = padded[..., 0].T.flatten()
    col_2 = padded[..., 1].T.flatten()
    col_1 = col_1[np.where(col_1 != pad_token[0])]
    col_2 = col_2[np.where(col_2 != pad_token[1])]
    return list(zip(col_1, col_2))


def resize_image_for_html(img_path, target_height=300):
    """
    We used this library instead of PIL or cv2 because many
    images were encoded in an unsupported format by those libraries.
    """
    try:
        with WandImage(filename=img_path) as img:
            aspect_ratio = img.width / img.height
            new_width = int(target_height * aspect_ratio)
            img.resize(new_width, target_height)
            resized_image_bytes = img.make_blob(format="webp")

        encoded_image = base64.b64encode(resized_image_bytes).decode()
        return f"data:image/webp;base64,{encoded_image}"

    except Exception as e:
        print(f"Error processing image: {e}")
        return None


def save_image(file_path, image_bytes, quality=80):
    if image_bytes is not None:
        try:
            with WandImage(blob=image_bytes) as img:
                img.quality = quality
                img.format = "webp"
                img.save(filename=file_path)
        except Exception:
            with open(file_path, "wb") as file:
                file.write(image_bytes)
        finally:
            return file_path


def get_image_path(data_dir, date, section_url):
    hash_url = hashlib.sha256(section_url.encode("utf-8")).hexdigest()
    try:
        year = date.year
        month = date.month
    except Exception as e:
        year = "unknown"
        month = "unknown"

    subdir = os.path.join(data_dir, str(year), str(month))
    os.makedirs(subdir, exist_ok=True)
    file_name = f"{hash_url}.webp"
    file_path = os.path.join(subdir, file_name)
    return file_path


def convert_count_to_str(count):
    if count >= 1000000:
        if not count % 1000000:
            return f"{count // 1000000}M"
        return f"{round(count / 1000000, 1)}M"

    if count >= 1000:
        if not count % 1000:
            return f"{count // 1000}K"
        return f"{round(count / 1000, 1)}K"

    return count


def execute(func):
    @wraps(func)
    def wrapper(engine, table, *args, **kwargs):
        ef_search = os.getenv("HNSW_EF_SEARCH", 1000)

        inspector = inspect(engine)
        if table not in inspector.get_table_names():
            return None

        metadata = MetaData()
        table_ref = Table(table, metadata, autoload_with=engine)

        with engine.connect() as connection:
            with connection.begin() as transaction:
                connection.execute(
                    text("SET LOCAL hnsw.ef_search = :ef_val"), {"ef_val": ef_search}
                )
                query = func(table_ref, *args, **kwargs)
                logger.debug(
                    f"{func.__name__}: "
                    + str(
                        query.compile(
                            engine,
                            compile_kwargs={"literal_binds": True},
                        )
                    )
                )
                result = connection.execute(query)
                if result.returns_rows:
                    rows = result.fetchall()
                    rows = clean_fetched_values(rows)
                    return rows
                else:
                    return result.rowcount

    return wrapper


def clean_fetched_values(results):
    if results:
        array = np.array(results)
        assert (
            len(array.shape) == 2
        ), f"There are {len(array.shape)} dimensions in the result"
        if array.shape[0] == 1 and array.shape[1] == 1:
            return array[0][0]
        if array.shape[0] > 1 and array.shape[1] == 1:
            return array.flatten().tolist()

        return array.tolist()

    return results


def is_image_url(url):
    image_pattern = re.compile(
        r"\.(jpg|jpeg|png|gif|bmp|svg|webp|tiff)$", re.IGNORECASE
    )
    return bool(image_pattern.search(url))


def get_embeddings(batch, embedding_url, timeout=20):

    payload = prepare_payload(batch)

    try:
        resp = requests.post(embedding_url, json=payload, timeout=timeout)
        resp.raise_for_status()
        data = orjson.loads(resp.content)
        embeddings = np.array(data["embeddings"])
        return embeddings

    except Exception as e:
        logger.error(f"Error fetching embeddings for batch of size {len(batch)}: {e}")
        return None


def prepare_payload(batch):
    data = []

    for el in batch:
        text = ""
        text += f"title: {el[DBCOLUMNS.title]}\n" if el[DBCOLUMNS.title] else ""
        text += f"content: {el[DBCOLUMNS.content]}\n" if el[DBCOLUMNS.content] else ""
        text += f"topic: {el[DBCOLUMNS.tag]}" if el[DBCOLUMNS.tag] else ""
        data.append(text)

    return {"data": data}


def get_query_embedding(query, embedding_url, timeout=20):
    try:
        resp = requests.post(embedding_url, json={"data": [query]}, timeout=timeout)
        resp.raise_for_status()
        data = orjson.loads(resp.content)
        embeddings = np.array(data["embeddings"])
        return embeddings.ravel().tolist()

    except Exception as e:
        logger.error(f"Error fetching embeddings for query {query}: {e}")
        return None
