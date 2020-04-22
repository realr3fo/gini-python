from resolver.resolver import ENDPOINT_URL, LIMITS
from resolver.unused.resolver_old import resolve_unbounded, resolve_property_gap
from utils.wikidata import get_results


# noinspection PyTypeChecker
def resolve_gini_analysis(limit):
    result_arr = []
    query_all_entities = "SELECT DISTINCT ?entity WHERE { ?s wdt:P31 ?entity . } LIMIT %d" % limit
    query_all_entities_result = get_results(ENDPOINT_URL, query_all_entities)
    all_entities_arr = query_all_entities_result["results"]["bindings"]
    counter = 0
    for elem in all_entities_arr:
        entity_link = elem["entity"]["value"]
        entity_id = entity_link.split("/")[-1]
        unbounded_result = resolve_unbounded(entity_id)
        entity_label = unbounded_result["instanceOf"]["entityLabel"]
        gini_coefficient = unbounded_result["gini"]
        amount = unbounded_result["amount"]
        entity_obj = {"entityID": entity_id, "entityLabel": entity_label, "gini_coefficient": gini_coefficient,
                      "entity_amount": amount}
        counter += 1
        result_arr.append(entity_obj)
    result = {"len": len(result_arr), "data": result_arr}
    return result


def resolve_property_gap_intersection_top_intersection_bot(entities):
    top_entities = []
    bottom_entities = []
    for elem in entities:
        if elem["percentile"] == "10%" or elem["percentile"] == "20%":
            bottom_entities.append(elem)
        elif elem["percentile"] == "100%" or elem["percentile"] == "90%":
            top_entities.append(elem)
    top_query = "SELECT DISTINCT ?prop ?propLabel { "
    counter = 0
    for elem in top_entities:
        if counter >= 6:
            break
        top_query += "wd:%s ?property []. " % elem["entity"]
        counter += 1
    top_query += """  FILTER(CONTAINS(STR(?property),"http://www.wikidata.org/prop/direct/"))
  FILTER NOT EXISTS {?prop wikibase:propertyType wikibase:ExternalId .}
  ?prop wikibase:directClaim ?property .
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". } 
}
"""
    query_results = get_results(ENDPOINT_URL, top_query)
    result_prop_arr = query_results["results"]["bindings"]
    top_prop_set = {}
    for elem in result_prop_arr:
        prop_link = elem['prop']['value']
        prop_id = prop_link.split("/")[-1]
        prop_label = elem['propLabel']["value"]
        prop_obj = (prop_id, prop_label, prop_link)
        top_prop_set[prop_id] = prop_obj

    bot_query = "SELECT DISTINCT ?prop ?propLabel { "
    counter = 0
    for elem in bottom_entities:
        if counter >= 6:
            break
        bot_query += "wd:%s ?property []. " % elem["entity"]
        counter += 1
    bot_query += """  FILTER(CONTAINS(STR(?property),"http://www.wikidata.org/prop/direct/"))
      FILTER NOT EXISTS {?prop wikibase:propertyType wikibase:ExternalId .}
      ?prop wikibase:directClaim ?property .
      SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". } 
    }
    """
    query_results = get_results(ENDPOINT_URL, bot_query)
    result_prop_arr = query_results["results"]["bindings"]
    bot_prop_set = {}
    for elem in result_prop_arr:
        prop_link = elem['prop']['value']
        prop_id = prop_link.split("/")[-1]
        prop_label = elem['propLabel']["value"]
        prop_obj = (prop_id, prop_label, prop_link)
        bot_prop_set[prop_id] = prop_obj

    top_prop_keys = list(top_prop_set.keys())
    for key in top_prop_keys:
        if key in bot_prop_set:
            del top_prop_set[key]

    result = []
    for elem in top_prop_set:
        prop_obj = top_prop_set[elem]
        prop_id = prop_obj[0]
        prop_label = prop_obj[1]
        prop_link = prop_obj[2]
        result.append({"property": prop_id, "propertyLabel": prop_label, "propertyLink": prop_link})
    result = {"len": len(result), "propertyGap": result}
    return result


def resolve_property_gap_intersection_top_union_bot(entities):
    top_entities = []
    bottom_entities = []
    for elem in entities:
        if elem["percentile"] == "10%" or elem["percentile"] == "20%":
            bottom_entities.append(elem)
        elif elem["percentile"] == "100%" or elem["percentile"] == "90%":
            top_entities.append(elem)
    top_query = "SELECT DISTINCT ?prop ?propLabel { "
    counter = 0
    if len(top_entities) > 50:
        top_entities = top_entities[:50]
    if len(bottom_entities) > 50:
        bottom_entities = bottom_entities[:50]
    for elem in top_entities:
        if counter >= 6:
            break
        top_query += "wd:%s ?property []. " % elem["entity"]
        counter += 1
    top_query += """  FILTER(CONTAINS(STR(?property),"http://www.wikidata.org/prop/direct/"))
      FILTER NOT EXISTS {?prop wikibase:propertyType wikibase:ExternalId .}
      ?prop wikibase:directClaim ?property .
      SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". } 
    }
    """
    query_results = get_results(ENDPOINT_URL, top_query)
    result_prop_arr = query_results["results"]["bindings"]
    top_prop_set = {}
    for elem in result_prop_arr:
        prop_link = elem['prop']['value']
        prop_id = prop_link.split("/")[-1]
        prop_label = elem['propLabel']["value"]
        prop_obj = (prop_id, prop_label, prop_link)
        top_prop_set[prop_id] = prop_obj

    bot_query = "select distinct ?prop ?propLabel { "
    for i in range(len(bottom_entities)):
        if i != 0:
            bot_query += "UNION "
        bot_query += "{wd:%s ?property ?o .} " % bottom_entities[i]["entity"]
    bot_query += """FILTER(CONTAINS(STR(?property),"http://www.wikidata.org/prop/direct/"))
      FILTER NOT EXISTS {?prop wikibase:propertyType wikibase:ExternalId .}
      ?prop wikibase:directClaim ?property .
      SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }  
    } LIMIT %s""" % LIMITS["property_gap"]
    query_results = get_results(ENDPOINT_URL, bot_query)
    result_prop_arr = query_results["results"]["bindings"]
    bot_prop_set = {}
    for elem in result_prop_arr:
        prop_link = elem['prop']['value']
        prop_id = prop_link.split("/")[-1]
        prop_label = elem['propLabel']["value"]
        prop_obj = (prop_id, prop_label, prop_link)
        bot_prop_set[prop_id] = prop_obj

    top_prop_keys = list(top_prop_set.keys())
    for key in top_prop_keys:
        if key in bot_prop_set:
            del top_prop_set[key]

    result = []
    for elem in top_prop_set:
        prop_obj = top_prop_set[elem]
        prop_id = prop_obj[0]
        prop_label = prop_obj[1]
        prop_link = prop_obj[2]
        result.append({"property": prop_id, "propertyLabel": prop_label, "propertyLink": prop_link})
    result = {"len": len(result), "propertyGap": result}
    return result


def resolve_property_gap_union_top_union_bot(entities):
    return resolve_property_gap(entities)
