import math

from utils.utils import chunks


# def calculate_gini(q_arr):
#     n = len(q_arr)
#     sum_prop = sum(elem[1] for elem in q_arr)
#     calculate_top_sum = sum((n + 1 - (i + 1)) * q_arr[i][1] for i in range(len(q_arr)))
#     right_below_gini_coefficient = n * sum_prop
#     right_top_gini_coefficient = 2 * calculate_top_sum
#     right_gini_coefficient = float(right_top_gini_coefficient) / float(right_below_gini_coefficient)
#     left_gini_coefficient = float(n + 1) / float(n)
#     gini_coefficient = left_gini_coefficient - right_gini_coefficient
#     return gini_coefficient


def calculate_gini(q_arr):
    from statistics import mean
    q_arr = [q_arr[i][1] for i in range(len(q_arr))]
    q_arr = sorted(q_arr, reverse=True)
    myu = mean(q_arr)
    n = len(q_arr)
    sum_y = sum((i + 1) * q_arr[i] for i in range(n))
    result = 1 + (1 / n) - ((2 / (n * n * myu)) * sum_y)
    return result


def get_chunked_arr(q_arr):
    chunk_size = float(len(q_arr)) / float(10)
    chunk_size = math.ceil(chunk_size)

    chunked_q_arr = chunks(q_arr, chunk_size)
    return chunked_q_arr


def normalize_data(cumulative_data):
    if len(cumulative_data) == 1:
        return [1]
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
    percentile_counter = 1
    counter = 1

    for elem in chunked_q_arr:
        for single_tuple in elem:
            cumulative += single_tuple[1]
            n = len(chunked_q_arr)
            if n != 10:
                percentile = math.ceil(10 * (percentile_counter - 0.5) / n)
                percentile = str(10 * percentile)
            else:
                percentile = str(counter * 10)
            entity_obj = {"entity": single_tuple[0], "propertyCount": single_tuple[1],
                          "label": single_tuple[2],
                          "percentile": percentile + "%",
                          "entityLink": single_tuple[3]}
            if len(single_tuple) == 5:
                entity_obj["entityProperties"] = single_tuple[4]
            if len(single_tuple) == 6:
                entity_obj["entityProperties"] = single_tuple[4]
                entity_obj["hasProperty"] = single_tuple[5]
            entities.append(entity_obj)

        percentile_counter += 1
        counter += 1
        cumulative_data.append(cumulative)
    return cumulative_data, entities
