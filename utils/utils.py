import numpy as np


def chunks(arr, size):
    result = []
    for i in range(0, len(arr), size):
        chunk = arr[i:i + size]
        result.append(chunk)
    return result


def interpolated(arr, num=10):
    x_loc = np.arange(len(arr))
    new_x_loc = np.linspace(0, len(arr), num)
    arr_interp = list(np.interp(new_x_loc, x_loc, arr))
    return arr_interp
