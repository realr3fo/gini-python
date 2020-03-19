import itertools

from gini import calculate_gini, normalize_data, get_chunked_arr, get_cumulative_data_and_entities, \
    calculate_gini_bounded, get_cumulative_data_and_entities_bounded
from wikidata import get_results

ENDPOINT_URL = "https://query.wikidata.org/sparql"
LIMITS = {"unbounded": 1000, "bounded": 300}


def get_instances_of(entity):
    entity_link, entity_id, entity_label, entity_desc = "", "", "", ""
    query = """SELECT ?entity ?entityLabel ?description {
      BIND(wd:%s AS ?entity)
      ?entity schema:description ?description.
      FILTER ( lang(?description) = "en" )
      SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
    }""" % entity
    query_results = get_results(ENDPOINT_URL, query)
    result_arr = query_results["results"]["bindings"]
    if len(result_arr) == 0:
        result = {
            "instancesOf": {"entityLink": entity_link, "entityID": entity_id, "entityLabel": entity_label,
                            "entityDescription": entity_desc},
            "gini": 0, "data": [], "entities": []}
        return result
    for elem in result_arr:
        entity_link = elem["entity"]["value"]
        entity_id = entity_link.split("/")[-1]
        entity_label = elem["entityLabel"]["value"]
        entity_desc = elem["description"]["value"]

    instance_of_data = {"entityLink": entity_link, "entityID": entity_id, "entityLabel": entity_label,
                        "entityDescription": entity_desc}

    return instance_of_data


def get_each_amount(chunked_q_arr):
    result = []
    for arr in chunked_q_arr:
        result.append(len(arr))
    return result


def get_insight(gini_coefficient, data, chunked_q_arr):
    if gini_coefficient > 0.5:
        max_idx = -1
        max_diff = -1
        for idx in range(len(data)):
            if idx + 1 >= len(data):
                break
            curr_diff = data[idx + 1] - data[idx]
            if curr_diff > max_diff:
                max_diff = curr_diff
                max_idx = idx

        result = "more than 0.5"
    else:
        result = "less than 0.5"
    return result


def resolve_unbounded(entity):
    instance_of_data = get_instances_of(entity)
    query = """SELECT DISTINCT ?item {  ?item wdt:P31 wd:%s} LIMIT %d""" % (entity, LIMITS["unbounded"])
    query_results = get_results(ENDPOINT_URL, query)
    item_arr = query_results["results"]["bindings"]
    if len(item_arr) == 0:
        result = {
            "instancesOf": {instance_of_data},
            "gini": 0, "data": [], "entities": []}
        return result

    q_arr = []
    for elem in item_arr:
        item_value = elem["item"]["value"]
        q_value = item_value.split("/")[-1]
        query = """
        SELECT DISTINCT ?item ?itemLabel (COUNT(DISTINCT(?p)) AS ?propertyCount) {
  BIND(wd:%s AS ?item)
  ?item ?p ?o . 
  FILTER(STRSTARTS(STR(?p), "http://www.wikidata.org/prop/direct/"))
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
} GROUP BY ?item ?itemLabel
        """ % q_value
        query_results = get_results(ENDPOINT_URL, query)
        single_instance = query_results["results"]["bindings"][0]
        property_count = int(single_instance["propertyCount"]["value"])
        item_label = single_instance["itemLabel"]["value"]
        q_arr.append((q_value, property_count, item_label))

    q_arr = sorted(q_arr, key=lambda x: x[1])

    gini_coefficient = calculate_gini(q_arr)
    gini_coefficient = round(gini_coefficient, 3)
    chunked_q_arr = get_chunked_arr(q_arr)
    each_amount = get_each_amount(chunked_q_arr)
    cumulative_data, entities = get_cumulative_data_and_entities(chunked_q_arr)
    data = normalize_data(cumulative_data)
    insight = get_insight(gini_coefficient, data, chunked_q_arr)

    result = {"instanceOf": instance_of_data, "limit": LIMITS, "gini": gini_coefficient, "each_amount": each_amount,
              "data": data,
              "insight": insight, "entities": entities}
    return result


def save_to_map(query_results, result_map):
    arr_results = query_results["results"]["bindings"]
    results_group = []
    for elem in arr_results:
        item_q_id = elem["item"]["value"].split("/")[-1]
        if item_q_id not in result_map:
            item_label = elem["itemLabel"]["value"]
            item_image = None
            if "image" in elem:
                item_image = elem["image"]["value"]
            result_map[item_q_id] = {"label": item_label, "image": item_image}
            results_group.append(item_q_id)
    return results_group


def get_each_amount_bounded(chunked_q_arr):
    result = []
    for elem in chunked_q_arr:
        result.append(len(elem))
    return result


def get_q_arr_bounded(results_array):
    result = []
    for idx in range(len(results_array)):
        for number in range(results_array[idx][1]):
            result.append((results_array[idx][0][number], idx))
    return result


def resolve_bounded(entity, properties):
    instance_of_data = get_instances_of(entity)
    properties = properties.split(",")
    combinations = []
    for i in range(1, len(properties) + 1):
        comb_of_len = []
        for comb in itertools.combinations(properties, i):
            comb_of_len.append(list(comb))
        combinations.append(comb_of_len)
    combinations = reversed(combinations)

    results_map = {}
    results_grouped_by_prop = []
    for comb in combinations:
        query = """select distinct ?item ?itemLabel (SAMPLE(?image) AS ?image) {{select DISTINCT ?item """
        for idx in range(len(comb)):
            prop_query = ""
            counter = 0
            for prop in comb[idx]:
                prop_query += " wdt:%s ?%d; " % (prop, counter)
                counter += 1
            if idx != 0:
                query += " UNION {?item wdt:P31 wd:%s; %s}" % (entity, prop_query)
            else:
                query += """{{ ?item wdt:P31 wd:%s; %s}""" % (entity, prop_query)
        query += """} LIMIT %d
  } OPTIONAL {?item wdt:P18 ?image} SERVICE wikibase:label { bd:serviceParam wikibase:language 
        "[AUTO_LANGUAGE],en". } } GROUP BY ?item ?itemLabel """ % LIMITS["bounded"]
        query_results = get_results(ENDPOINT_URL, query)
        results_group = save_to_map(query_results, results_map)
        results_grouped_by_prop.append((results_group, len(results_group)))

    query = """select distinct ?item ?itemLabel (SAMPLE(?image) AS ?image) {
  {
    select DISTINCT ?item {
    ?item wdt:P31 wd:%s.
    } LIMIT %d
  } OPTIONAL {?item wdt:P18 ?image} """ % (entity, LIMITS["bounded"])
    for prop in properties:
        query += "  FILTER NOT EXISTS {?item wdt:%s ?0} " % prop
    query += """  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }}
    GROUP BY ?item ?itemLabel """
    query_results = get_results(ENDPOINT_URL, query)
    results_group = save_to_map(query_results, results_map)
    results_grouped_by_prop.append((results_group, len(results_group)))
    reversed_results_grouped_by_prop = list(reversed(results_grouped_by_prop))
    # q_arr = sorted(results_grouped_by_prop, key=lambda x: x[1])
    q_arr = get_q_arr_bounded(reversed_results_grouped_by_prop)
    gini_coefficient = calculate_gini_bounded(q_arr)
    gini_coefficient = round(gini_coefficient, 3)
    chunked_q_arr = get_chunked_arr(q_arr)
    each_amount = get_each_amount_bounded(chunked_q_arr)
    cumulative_data, entities = get_cumulative_data_and_entities_bounded(chunked_q_arr, results_map)
    data = normalize_data(cumulative_data)
    insight = get_insight(gini_coefficient, data, chunked_q_arr)

    result = {"instanceOf": instance_of_data, "insight": insight, "limit": LIMITS,
              "gini": gini_coefficient, "each_amount": each_amount,
              "data": data, "entities": entities}
    return result
