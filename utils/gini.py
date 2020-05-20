import math
import re
import urllib.parse

from utils.utils import chunks, interpolated


def calculate_gini(q_arr):
    from statistics import mean
    q_arr = [q_arr[i][1] for i in range(len(q_arr))]
    q_arr = sorted(q_arr, reverse=True)
    myu = mean(q_arr)
    n = len(q_arr)
    sum_y = sum((i + 1) * q_arr[i] for i in range(n))
    result = 1 + (1 / n) - ((2 / (n * n * myu)) * sum_y)
    return result


def get_chunked_arr(q_arr, num=10):
    chunk_size = float(len(q_arr)) / float(num)
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


def get_ten_percentile(data):
    n = len(data)
    percentiles = []
    for i in range(n):
        percentile = 10 * ((i + 1) - 0.5) / n
        percentile = math.ceil(percentile)
        percentiles.append(str(percentile * 10) + "%")
    return percentiles


def get_insight(data):
    data_length = len(data) - 1
    eight_percentile = round(0.8 * data_length)
    percentile_eight_data = data[eight_percentile]
    gap_diff = 1.0 - percentile_eight_data
    gap_percentage = gap_diff * 100
    gap_rounded = round(gap_percentage)

    result = "The top 20%% population of the class amounts to %d%% cumulative number of properties." % gap_rounded
    return result


# noinspection PyTypeChecker
def get_new_histogram_data(q_arr):
    property_count = [q_arr[i][1] for i in range(len(q_arr))]
    import numpy as np
    uniq_keys = np.unique(property_count)
    bins = uniq_keys.searchsorted(property_count)
    bins = np.bincount(bins)
    actual_s = [int(elem) for elem in uniq_keys]
    max_num = max(actual_s)
    labels = []
    for elem in actual_s:
        label = elem * 100 / max_num
        labels.append(round(label, 2))

    entities_data = [int(elem) for elem in bins]

    result = {"label": labels, "actual": actual_s, "data": entities_data}
    return result


def construct_results_gini(q_arr, query=""):
    from resolver.resolver import LIMITS

    q_arr = sorted(q_arr, key=lambda x: x[1])
    gini_coefficient = calculate_gini(q_arr)
    gini_coefficient = round(gini_coefficient, 3)
    if len(q_arr) >= LIMITS["unbounded"]:
        exceed_limit = True
    else:
        exceed_limit = False

    chunked_q_arr = get_chunked_arr(q_arr)
    each_amount = []
    count = 0
    for arr in chunked_q_arr:
        count += len(arr)
        each_amount.append(count)
    cumulative_data, entities = get_cumulative_data_and_entities(chunked_q_arr)
    from collections import Counter
    property_counts = Counter(item['propertyCount'] for item in entities if item.get('propertyCount'))
    histogram_data = [count for _, count in property_counts.items()]
    if len(histogram_data) > 10:
        chunked_histogram_arr = get_chunked_arr(histogram_data)
        histogram_data = []
        for elem in chunked_histogram_arr:
            histogram_data.append(sum(elem))
    histogram_data = interpolated(histogram_data)
    histogram_data.insert(0, 0)
    original_data = list(cumulative_data)
    cumulative_data.insert(0, 0)
    data = normalize_data(cumulative_data)
    for idx in range(len(data)):
        max_num = 0.1 * idx
        if data[idx] > max_num:
            data[idx] = max_num
    insight = get_insight(data)
    percentiles = get_ten_percentile(original_data)
    percentiles.insert(0, '0%')

    new_histogram_data = get_new_histogram_data(q_arr)

    query_link = "https://query.wikidata.org/#" + urllib.parse.quote(query)

    result = {"limit": LIMITS, "query_link": query_link, "amount": each_amount[-1], "gini": gini_coefficient,
              "each_amount": each_amount, "histogramData": histogram_data, "newHistogramData": new_histogram_data,
              "data": data, "exceedLimit": exceed_limit, "percentileData": percentiles,
              "insight": insight, "entities": entities}
    return result
