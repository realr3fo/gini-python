import itertools

from gini import calculate_gini, normalize_data, get_chunked_arr, get_cumulative_data_and_entities, \
    calculate_gini_bounded, get_cumulative_data_and_entities_bounded
from wikidata import get_results


def resolve_unbounded(entity):
    endpoint_url = "https://query.wikidata.org/sparql"

    query = """SELECT distinct ?item ?itemLabel (SAMPLE(?image) AS ?image){?item wdt:P31 wd:%s. OPTIONAL { ?item wdt:P18 
        ?image}FILTER(STRSTARTS(STR(wdt:P18),"http://www.wikidata.org/prop/direct/"))SERVICE wikibase:label { 
        bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }} GROUP BY ?item ?itemLabel""" % entity
    query_results = get_results(endpoint_url, query)
    item_arr = query_results["results"]["bindings"]
    if len(item_arr) == 0:
        result = {"gini": 0, "data": [], "entities": []}
        return result

    q_arr = []
    for elem in item_arr:
        item_value = elem["item"]["value"]
        item_label = elem["itemLabel"]["value"]
        item_image = None
        if "image" in elem:
            item_image = elem["image"]["value"]
        q_value = item_value.split("/")[-1]
        query = """SELECT (COUNT(DISTINCT(?p)) AS ?propertyCount) {wd:%s ?p ?o .FILTER(STRSTARTS(STR(?p),
            "http://www.wikidata.org/prop/direct/"))}""" % q_value
        query_results = get_results(endpoint_url, query)
        property_count = int(query_results["results"]["bindings"][0]["propertyCount"]["value"])
        q_arr.append((q_value, property_count, item_label, item_image))

    q_arr = sorted(q_arr, key=lambda x: x[1])

    gini_coefficient = calculate_gini(q_arr)
    chunked_q_arr = get_chunked_arr(q_arr)
    cumulative_data, entities = get_cumulative_data_and_entities(chunked_q_arr)
    data = normalize_data(cumulative_data)

    result = {"gini": gini_coefficient, "data": data, "entities": entities}
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


def resolve_bounded(entity, properties):
    endpoint_url = "https://query.wikidata.org/sparql"
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
        query = """select distinct ?item ?itemLabel (SAMPLE(?image) AS ?image)"""
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
            # UNION
        query += """ OPTIONAL {?item wdt:P18 ?image} SERVICE wikibase:label { bd:serviceParam wikibase:language 
        "[AUTO_LANGUAGE],en". } } GROUP BY ?item ?itemLabel """
        query_results = get_results(endpoint_url, query)
        results_group = save_to_map(query_results, results_map)
        results_grouped_by_prop.append((results_group, len(results_group)))

    # send one more to wikidata for the rest of data that does not have all of those properties
    query = """select distinct ?item ?itemLabel (SAMPLE(?image) AS ?image) {?item wdt:P31 wd:%s; 
    OPTIONAL {?item wdt:P18 ?image} """ % entity
    for prop in properties:
        query += "  FILTER NOT EXISTS {?item wdt:%s ?0} " % prop
    query += """  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }}
    GROUP BY ?item ?itemLabel """
    query_results = get_results(endpoint_url, query)
    results_group = save_to_map(query_results, results_map)
    results_grouped_by_prop.append((results_group, len(results_group)))

    for i in results_grouped_by_prop:
        print(i)

    q_arr = sorted(results_grouped_by_prop, key=lambda x: x[1])

    gini_coefficient = calculate_gini_bounded(q_arr)
    chunked_q_arr = get_chunked_arr(q_arr)
    cumulative_data, entities = get_cumulative_data_and_entities_bounded(chunked_q_arr, results_map)
    data = normalize_data(cumulative_data)

    result = {"gini": gini_coefficient, "data": data, "entities": entities}
    return result
