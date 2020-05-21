import calendar
import csv
import math
import re
import time
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
        percentile = (i + 1) / n
        percentile = round(percentile, 1)
        percentiles.append(str(int(percentile * 100)) + "%")
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


def remove_zeros(arr):
    prev = 0
    for i in range(len(arr)):
        if arr[i] == 0:
            arr[i] = prev
        prev = arr[i]
    return arr


def get_new_histogram_data(q_arr):
    property_count = [q_arr[i][1] for i in range(len(q_arr))]
    import numpy as np
    uniq_keys = np.unique(property_count)
    bins = uniq_keys.searchsorted(property_count)
    bins = np.bincount(bins)
    actual_s = [int(elem) for elem in uniq_keys]
    entities_data = [int(elem) for elem in bins]
    max_num = max(actual_s)
    labels = [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
    actual_result = [[0]]
    range_arr = []
    for elem in labels:
        calculate_range = elem * 0.01 * max_num
        range_arr.append(calculate_range)
    range_before = 0
    entities_data_result = [0]
    count = 0
    range_arr = range_arr[1:]
    for single_range in range_arr:
        current_actual_s = []
        current_count = 0
        for elem in zip(actual_s, entities_data):
            if range_before < elem[0] <= single_range:
                current_actual_s.append(elem[0])
                current_count += elem[1]
        count += 1
        range_before = single_range
        actual_result.append(current_actual_s)
        entities_data_result.append(current_count)
    show = []
    for elem in entities_data_result:
        if elem == 0:
            show.append(0)
        else:
            show.append(1)

    entities_data_result = remove_zeros(entities_data_result)

    result = {"label": labels, "actual": actual_result, "data": entities_data_result, "show": show}
    return result


def construct_results_gini(q_arr, source="", query=""):
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
    n = len(original_data)
    absolute_line = []
    for i in range(n):
        percentile = (i + 1) / n
        percentile = round(percentile, 1)
        absolute_line.append(percentile)
    absolute_line.insert(0, 0)
    for idx in range(len(data)):
        max_num = absolute_line[idx]
        if max_num < data[idx]:
            data[idx] = max_num

    insight = get_insight(data)
    percentiles = get_ten_percentile(original_data)
    percentiles.insert(0, '0%')

    new_histogram_data = get_new_histogram_data(q_arr)

    query_link = "https://query.wikidata.org/#" + urllib.parse.quote(query)

    from flask import request
    from main import app
    sparql_download_link = ""
    if source == "profile":
        ts = calendar.timegm(time.gmtime())
        file_name = app.config["CLIENT_SPARQL"] + "/" + str(ts) + ".sparql"
        with open(file_name, "w") as fo:
            fo.write(query)
        sparql_download_link = request.url_root + "api/download/sparql?file_name=" + str(ts) + ".sparql"

    csv_download_link = ""
    if source == "profile":
        ts = calendar.timegm(time.gmtime())
        file_name = app.config["CLIENT_CSV"] + "/" + str(ts) + ".csv"
        with open(file_name, "w") as fo:
            file_writer = csv.writer(fo, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            file_writer.writerow(['entityID', 'entityLabel', 'entityLink', 'entityPropertyCount'])
            for entity in entities:
                file_writer.writerow([entity["entity"], entity["label"], entity["entityLink"], entity["propertyCount"]])
        csv_download_link = request.url_root + "api/download/csv?file_name=" + str(ts) + ".csv"

    result = {"limit": LIMITS, "query_link": query_link, "sparql_download_link": sparql_download_link,
              "csv_download_link": csv_download_link, "amount": each_amount[-1], "gini": gini_coefficient,
              "each_amount": each_amount, "histogramData": histogram_data, "newHistogramData": new_histogram_data,
              "data": data, "exceedLimit": exceed_limit, "percentileData": percentiles,
              "insight": insight, "entities": entities}
    return result
