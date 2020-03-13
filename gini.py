import math

from utils import chunks


def calculate_gini(q_arr):
    n = len(q_arr)
    sum_prop = sum(n for _, n in q_arr)
    calculate_top_sum = sum((n + 1 - (i + 1)) * q_arr[i][1] for i in range(len(q_arr)))
    right_below_gini_coefficient = n * sum_prop
    right_top_gini_coefficient = 2 * calculate_top_sum
    right_gini_coefficient = float(right_top_gini_coefficient) / float(right_below_gini_coefficient)
    left_gini_coefficient = float(n + 1) / float(n)
    gini_coefficient = left_gini_coefficient - right_gini_coefficient
    return gini_coefficient


def get_chunked_arr(q_arr):
    chunk_size = float(len(q_arr)) / float(10)
    chunk_size = math.ceil(chunk_size)

    chunked_q_arr = chunks(q_arr, chunk_size)
    return chunked_q_arr


def normalize_data(cumulative_data):
    min_cum_data = min(cumulative_data)
    max_cum_data = max(cumulative_data)
    data = []
    for elem in cumulative_data:
        normalized_data = float(elem - min_cum_data) / float(max_cum_data - min_cum_data)
        data.append(normalized_data)
    return data

def get_cumulative_data_and_entities(chunked_q_arr):
    cumulative_data = []
    cumulative = 0
    entities = []

    for elem in chunked_q_arr:
        chunked_entity = []
        for single_tuple in elem:
            cumulative += single_tuple[1]
            chunked_entity.append({"entity": single_tuple[0], "propertyCount": single_tuple[1]})
        cumulative_data.append(cumulative)
        entities.append(chunked_entity)

    return cumulative_data, entities
