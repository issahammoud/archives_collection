import itertools
import numpy as np


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
