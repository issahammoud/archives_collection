import itertools
import numpy as np


def alternate_elements(list_of_list):
    pad_token = "to_delete"
    padded = list(zip(*itertools.zip_longest(*list_of_list, fillvalue=pad_token)))
    array = np.array(padded).T.flatten()
    array = array[np.where(array != pad_token)]
    return array
