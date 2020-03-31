import math
import random
import time

from gini import calculate_gini, normalize_data, get_chunked_arr, get_cumulative_data_and_entities
from main import db
from models import Logs
from wikidata import get_results

ENDPOINT_URL = "https://query.wikidata.org/sparql"
LIMITS = {"unbounded": 10000, "bounded": 10000, "property_gap": 1000}


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


def resolve_property_gap(entities):
    sample_entity_obj = entities[0]
    chunked_entities = get_chunked_arr(entities)
    if "properties" in sample_entity_obj:
        pass
        # return resolve_bounded_property_gap(chunked_entities)
    else:
        return get_unbounded_property_gap(chunked_entities)


def get_unbounded_property_gap(chunked_q_arr):
    data_length = len(chunked_q_arr)
    if data_length <= 1:
        return []
    top_percentile = math.floor(0.8 * data_length)
    top_arr = []
    for i in range(top_percentile, data_length):
        for elem in chunked_q_arr[i]:
            if len(top_arr) >= 25:
                break
            top_arr.append(elem)
    top_query = "SELECT DISTINCT ?p ?pLabel { "
    for i in range(len(top_arr)):
        if i != 0:
            top_query += "UNION "
        top_query += "{wd:%s ?property ?o .} " % top_arr[i]["entity"]
    top_query += """FILTER(CONTAINS(STR(?property),"http://www.wikidata.org/prop/direct/"))
  FILTER NOT EXISTS {?p wikibase:propertyType wikibase:ExternalId .}
  ?p wikibase:directClaim ?property .
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". } 
} LIMIT %s""" % LIMITS["property_gap"]
    query_results = get_results(ENDPOINT_URL, top_query)
    top_prop_arr = query_results["results"]["bindings"]
    if len(top_prop_arr) == 0:
        return []
    top_prop_set = {}
    for elem in top_prop_arr:
        prop_link = elem['p']['value']
        prop_id = prop_link.split("/")[-1]
        prop_label = elem['pLabel']["value"]
        prop_obj = (prop_id, prop_label, prop_link)
        top_prop_set[prop_id] = prop_obj

    # print(top_prop_set)
    bot_percentile = math.ceil(0.2 * data_length)
    bot_arr = []
    for i in range(0, bot_percentile):
        for elem in chunked_q_arr[i]:
            if len(bot_arr) >= 25:
                break
            bot_arr.append(elem)
    bot_query = "select distinct ?p ?pLabel { "
    for i in range(len(bot_arr)):
        if i != 0:
            bot_query += "UNION "
        bot_query += "{wd:%s ?property ?o .} " % bot_arr[i]["entity"]
    bot_query += """FILTER(CONTAINS(STR(?property),"http://www.wikidata.org/prop/direct/"))
  FILTER NOT EXISTS {?p wikibase:propertyType wikibase:ExternalId .}
  ?p wikibase:directClaim ?property .
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }  
} LIMIT %s""" % LIMITS["property_gap"]
    query_results = get_results(ENDPOINT_URL, bot_query)
    bot_prop_arr = query_results["results"]["bindings"]
    if len(bot_prop_arr) == 0:
        return []
    bot_prop_set = {}
    for elem in bot_prop_arr:
        prop_link = elem['p']['value']
        prop_id = prop_link.split("/")[-1]
        prop_label = elem['pLabel']["value"]
        prop_obj = (prop_id, prop_label, prop_link)
        bot_prop_set[prop_id] = prop_obj

    for elem in bot_prop_set:
        if elem in top_prop_set:
            del top_prop_set[elem]

    result = []
    for elem in top_prop_set:
        prop_obj = top_prop_set[elem]
        prop_id = prop_obj[0]
        prop_label = prop_obj[1]
        prop_link = prop_obj[2]
        result.append({"property": prop_id, "propertyLabel": prop_label, "propertyLink": prop_link})
    result = {"propertyGap": result}
    return result


def get_insight(data, chunked_q_arr):
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


def resolve_unbounded(entity):
    instance_of_data = get_instances_of(entity)
    query = """
    SELECT ?item ?itemLabel ?cnt {
        {SELECT ?item (COUNT(DISTINCT(?prop)) AS ?cnt) {
        
        {SELECT DISTINCT ?item WHERE {
           ?item wdt:P31 wd:%s .
        } LIMIT %d}
        OPTIONAL { ?item ?p ?o . FILTER(CONTAINS(STR(?p),"http://www.wikidata.org/prop/direct/")) 
        ?prop wikibase:directClaim ?p . FILTER NOT EXISTS {?prop wikibase:propertyType wikibase:ExternalId .} }
                
        } GROUP BY ?item}
        
        SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
          
        } ORDER BY DESC(?cnt)
    """ % (entity, LIMITS["unbounded"])
    query_results = get_results(ENDPOINT_URL, query)
    item_arr = query_results["results"]["bindings"]
    if len(item_arr) == 0:
        result = {
            "instancesOf": {instance_of_data},
            "gini": 0, "data": [], "entities": []}
        return result
    q_arr = []
    for elem in item_arr:
        item_link = elem['item']['value']
        item_id = item_link.split("/")[-1]
        property_count = int(elem['cnt']['value'])
        item_label = elem['itemLabel']['value']
        entity_obj = (item_id, property_count, item_label, item_link)
        q_arr.append(entity_obj)

    q_arr = sorted(q_arr, key=lambda x: x[1])
    gini_coefficient = calculate_gini(q_arr)
    gini_coefficient = round(gini_coefficient, 3)
    if len(q_arr) >= LIMITS["unbounded"]:
        exceed_limit = True
    else:
        exceed_limit = False

    chunked_q_arr = get_chunked_arr(q_arr)
    each_amount = get_each_amount(chunked_q_arr)
    cumulative_data, entities = get_cumulative_data_and_entities(chunked_q_arr)
    data = normalize_data(cumulative_data)
    insight = get_insight(data, chunked_q_arr)
    percentiles = get_ten_percentile(data)
    # property_gap = get_property_gap(chunked_q_arr)
    result = {"instanceOf": instance_of_data, "limit": LIMITS, "gini": gini_coefficient, "each_amount": each_amount,
              "data": data, "exceedLimit": exceed_limit, "percentileData": percentiles,
              "insight": insight, "entities": entities}
    save_logs_to_db({"entity": entity, "properties": ""})
    return result


def get_each_amount_bounded(chunked_q_arr):
    result = []
    for elem in chunked_q_arr:
        result.append(len(elem))
    return result


def resolve_bounded(entity, properties_request):
    instance_of_data = get_instances_of(entity)
    properties = properties_request.split(",")
    new_properties = []
    for elem in properties:
        new_elem = elem.strip()
        new_properties.append(new_elem)
    properties = new_properties

    jml_join = " + ?".join(properties)

    query = "SELECT DISTINCT ?item ?itemLabel "
    for elem in properties:
        query += "?%s " % elem
    query += "(?%s AS ?count) {" % jml_join
    query += "{ SELECT ?item "
    for elem in properties:
        query += "?%s " % elem
    query += "WHERE{ { SELECT ?item WHERE { ?item wdt:P31 wd:%s . } LIMIT %d}" % (entity, LIMITS["bounded"])
    for i in range(len(properties)):
        query += "OPTIONAL { ?item wdt:%s _:v%d . BIND (1 AS ?%s) } " % (properties[i], i, properties[i])
    for elem in properties:
        query += "OPTIONAL { BIND (0 AS ?%s) } " % elem
    query += """}} SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }}"""
    query_results = get_results(ENDPOINT_URL, query)
    item_arr = query_results["results"]["bindings"]
    q_arr = []
    for elem in item_arr:
        item_link = elem["item"]["value"]
        item_id = item_link.split("/")[-1]
        property_count = int(elem["count"]["value"])
        item_label = elem["itemLabel"]["value"]
        entity_obj = (item_id, property_count, item_label, item_link)
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
    data = normalize_data(cumulative_data)
    insight = get_insight(data, chunked_q_arr)
    percentiles = get_ten_percentile(data)
    # property_gap = get_property_gap(chunked_q_arr)
    result = {"instanceOf": instance_of_data, "insight": insight, "limit": LIMITS,
              "gini": gini_coefficient, "each_amount": each_amount, "exceedLimit": exceed_limit,
              "percentileData": percentiles,
              "data": data, "entities": entities}
    save_logs_to_db({"entity": entity, "properties": properties_request})

    return result


def save_logs_to_db(data):
    entity = data['entity']
    properties = data['properties']
    timestamp = str(time.time())
    try:
        logs = Logs(
            entity=entity,
            properties=properties,
            timestamp=timestamp
        )
        db.session.add(logs)
        db.session.commit()
    except Exception as e:
        return str(e)
