import base64
import hashlib
import itertools
import numpy as np
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


def resize_image_for_html(image_bytes, target_height=300):
    """
    We used this library instead of PIL or cv2 because many
    images were encoded in an unsupported format by those libraries.
    """
    try:
        with WandImage(blob=image_bytes) as img:
            aspect_ratio = img.width / img.height
            new_width = int(target_height * aspect_ratio)
            img.resize(new_width, target_height)
            resized_image_bytes = img.make_blob(format="png")

        encoded_image = base64.b64encode(resized_image_bytes).decode()
    except Exception as e:
        print(f"Error processing image: {e}")
        encoded_image = base64.b64encode(image_bytes).decode("utf-8")

    return f"data:image/png;base64,{encoded_image}"


def convert_count_to_str(count):
    full_str = f"{count:,}"
    if count >= 1000000:
        if not count % 1000000:
            return full_str, f"{count // 1000000}M"
        return full_str, f"{round(count / 1000000, 1)}M"

    if count >= 1000:
        if not count % 1000:
            return full_str, f"{count // 1000}K"
        return full_str, f"{round(count / 1000, 1)}K"

    return full_str, count
