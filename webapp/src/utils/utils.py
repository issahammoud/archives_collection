import re
import os
import base64
import hashlib
import itertools
import numpy as np
from functools import wraps
from sqlalchemy import inspect
from spellchecker import SpellChecker
from wand.image import Image as WandImage


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


def hash_url(url):
    return hashlib.sha256(url.encode("utf-8")).hexdigest()


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


def get_image_path(data_dir, date, rowid):
    try:
        year = date.year
        month = date.month
    except Exception as e:
        year = "unknown"
        month = "unknown"

    subdir = os.path.join(data_dir, str(year), str(month))
    os.makedirs(subdir, exist_ok=True)
    file_name = f"{rowid}.webp"
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


def has_table_decorator(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        engine = engine = kwargs.get("engine", args[0])
        table = kwargs.get("table", args[1])

        if not engine or not table:
            raise ValueError(
                "Both 'engine' and 'table_name' must be provided as arguments."
            )

        inspector = inspect(engine)
        if table not in inspector.get_table_names():
            return None

        return func(*args, **kwargs)

    return wrapper


def prepare_query(query):
    concatenated_words = []
    spell = SpellChecker()
    words = query.split()

    new_words = [spell.correction(word) for word in words]

    for word1, word2 in zip(words, new_words):
        if word2:
            concatenated_words.append(f"({word1} | {word2})")
        else:
            concatenated_words.append(word1)

    return " & ".join(concatenated_words)


def is_image_url(url):
    image_pattern = re.compile(
        r"\.(jpg|jpeg|png|gif|bmp|svg|webp|tiff)$", re.IGNORECASE
    )
    return bool(image_pattern.search(url))
