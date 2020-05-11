import asyncio
import math

from utils.gini import calculate_gini, get_chunked_arr, get_cumulative_data_and_entities, normalize_data
from utils.wikidata import get_results, async_get_results


def get_insight(data):
    data_length = len(data) - 1
    eight_percentile = round(0.8 * data_length)
    percentile_eight_data = data[eight_percentile]
    gap_diff = 1.0 - percentile_eight_data
    gap_percentage = gap_diff * 100
    gap_rounded = round(gap_percentage)

    result = "The top 20%% population of the class amounts to %d%% cumulative number of properties." % gap_rounded
    return result


def get_ten_percentile(data):
    n = len(data)
    percentiles = []
    for i in range(n):
        percentile = 10 * ((i + 1) - 0.5) / n
        percentile = math.ceil(percentile)
        percentiles.append(str(percentile * 10) + "%")
    return percentiles


def get_each_amount_bounded(chunked_q_arr):
    result = []
    for elem in chunked_q_arr:
        result.append(len(elem))
    return result


async def get_gini_from_wikidata(entity, filter_query, has_property_query, offset_count):
    from resolver.resolver import LIMITS, ENDPOINT_URL
    limit = 1000
    offset = offset_count * 1000
    query = """
            SELECT ?item ?itemLabel ?cnt ?p1 {
                {SELECT ?item (COUNT(DISTINCT(?prop)) AS ?cnt) ?p1 {

                {SELECT DISTINCT ?item WHERE {
                   ?item wdt:P31 wd:%s .
                   %s
                } LIMIT %d OFFSET %d}
                OPTIONAL { ?item ?p ?o . FILTER(CONTAINS(STR(?p),"http://www.wikidata.org/prop/direct/")) 
                ?prop wikibase:directClaim ?p . FILTER NOT EXISTS {?prop wikibase:propertyType wikibase:ExternalId .} }
                %s

                } GROUP BY ?item ?p1}

                SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }

                } ORDER BY DESC(?cnt)
            """ % (entity, filter_query, limit, offset, has_property_query)
    query_results = await async_get_results(ENDPOINT_URL, query)
    return query_results["results"]["bindings"]


def resolve_gini_with_filters_unbounded(entity, filters, has_property):
    from resolver.resolver import LIMITS, ENDPOINT_URL
    filter_query = ""
    for elem in filters:
        for elem_filter in elem.keys():
            filter_query += "?item wdt:%s wd:%s . " % (elem_filter, elem[elem_filter])
    has_property_query = ""
    if has_property != "" and has_property is not None:
        has_property_query += " OPTIONAL { ?item wdt:%s _:v1 . BIND (1 AS ?p1) } OPTIONAL { BIND (0 AS ?p1) } " % has_property
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    tasks = []
    # create instance of Semaphore
    for i in range(10):
        task = loop.create_task(get_gini_from_wikidata(entity, filter_query, has_property_query, i))
        tasks.append(task)
    query_results_arr = loop.run_until_complete(asyncio.gather(*tasks))
    loop.close()
    query_results_bindings = [item for sublist in query_results_arr for item in sublist]
    item_arr = query_results_bindings
    if len(item_arr) == 0:
        result = {
            "gini": 0, "data": [], "entities": []}
        return result
    q_arr = []
    for elem in item_arr:
        item_link = elem['item']['value']
        item_id = item_link.split("/")[-1]
        property_count = int(elem['cnt']['value'])
        item_label = elem['itemLabel']['value']
        item_has_property = None
        if 'p1' in elem:
            item_has_property = elem['p1']['value']
            if item_has_property == '1':
                item_has_property = True
            else:
                item_has_property = False
        entity_obj = (item_id, property_count, item_label, item_link, item_has_property)
        q_arr.append(entity_obj)
    q_arr = sorted(q_arr, key=lambda x: x[1])
    gini_coefficient = calculate_gini(q_arr)
    gini_coefficient = round(gini_coefficient, 3)
    if len(q_arr) >= LIMITS["unbounded"]:
        exceed_limit = True
    else:
        exceed_limit = False

    chunked_q_arr = get_chunked_arr(q_arr)
    each_amount = []
    for arr in chunked_q_arr:
        each_amount.append(len(arr))
    cumulative_data, entities = get_cumulative_data_and_entities(chunked_q_arr)
    from collections import Counter
    property_counts = Counter(item['propertyCount'] for item in entities if item.get('propertyCount'))
    histogram_data = [count for _, count in property_counts.items()]
    if len(histogram_data) > 10:
        chunked_histogram_arr = get_chunked_arr(histogram_data)
        histogram_data = []
        for elem in chunked_histogram_arr:
            histogram_data.append(sum(elem))
    histogram_data.insert(0, 0)
    original_data = list(cumulative_data)
    cumulative_data.insert(0, 0)
    data = normalize_data(cumulative_data)
    insight = get_insight(data)
    percentiles = get_ten_percentile(original_data)
    percentiles.insert(0, '0%')
    result = {"limit": LIMITS, "amount": sum(each_amount), "gini": gini_coefficient,
              "each_amount": each_amount, "histogramData": histogram_data,
              "data": data, "exceedLimit": exceed_limit, "percentileData": percentiles,
              "insight": insight, "entities": entities}
    return result


def resolve_gini_with_filters_bounded(entity_id, filters, properties, has_property):
    properties = properties
    new_properties = []
    for elem in properties:
        new_elem = elem.strip()
        new_properties.append(new_elem)
    properties = new_properties

    jml_join = " + ?".join(properties)

    filter_query = ""
    for elem in filters:
        for elem_filter in elem.keys():
            filter_query += "?item wdt:%s wd:%s . " % (elem_filter, elem[elem_filter])

    query = "SELECT DISTINCT ?item ?itemLabel ?p1 "
    for elem in properties:
        query += "?%s " % elem
    query += "(?%s AS ?count) {" % jml_join
    query += "{ SELECT ?item ?p1 "
    for elem in properties:
        query += "?%s " % elem
    from resolver.resolver import LIMITS
    query += "WHERE{ { SELECT ?item WHERE { ?item wdt:P31 wd:%s . %s } LIMIT %d}" % (
        entity_id, filter_query, LIMITS["bounded"])
    for i in range(len(properties)):
        query += "OPTIONAL { ?item wdt:%s _:v%d . BIND (1 AS ?%s) } " % (properties[i], i, properties[i])
    for elem in properties:
        query += "OPTIONAL { BIND (0 AS ?%s) } " % elem
    if has_property != "" and has_property is not None:
        query += " OPTIONAL { ?item wdt:%s _:v-0 . BIND (1 AS ?p1) } OPTIONAL { BIND (0 AS ?p1) } " % has_property
    query += """}} SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }}"""
    from resolver.resolver import ENDPOINT_URL
    query_results = get_results(ENDPOINT_URL, query)
    item_arr = query_results["results"]["bindings"]
    q_arr = []
    for elem in item_arr:
        item_link = elem["item"]["value"]
        item_id = item_link.split("/")[-1]
        property_count = int(elem["count"]["value"])
        entity_props = []
        for prop in properties:
            if elem[prop]["value"] == "1":
                entity_props.append(prop)
        item_label = elem["itemLabel"]["value"]
        item_has_property = None
        if 'p1' in elem:
            item_has_property = elem['p1']['value']
            if item_has_property == '1':
                item_has_property = True
            else:
                item_has_property = False
        entity_obj = (item_id, property_count, item_label, item_link, entity_props, item_has_property)
        q_arr.append(entity_obj)
    q_arr = sorted(q_arr, key=lambda x: x[1])

    if len(q_arr) >= LIMITS["bounded"]:
        exceed_limit = True
    else:
        exceed_limit = False

    gini_coefficient = calculate_gini(q_arr)
    gini_coefficient = round(gini_coefficient, 3)
    chunked_q_arr = get_chunked_arr(q_arr)
    each_amount = get_each_amount_bounded(chunked_q_arr)
    cumulative_data, entities = get_cumulative_data_and_entities(chunked_q_arr)
    original_data = list(cumulative_data)
    cumulative_data.insert(0, 0)
    data = normalize_data(cumulative_data)
    insight = get_insight(original_data)
    percentiles = get_ten_percentile(original_data)
    percentiles.insert(0, '0%')
    # property_gap = get_property_gap(chunked_q_arr)
    result = {"insight": insight, "limit": LIMITS, "amount": sum(each_amount),
              "gini": gini_coefficient, "each_amount": each_amount, "exceedLimit": exceed_limit,
              "percentileData": percentiles,
              "data": data, "entities": entities}
    return result
