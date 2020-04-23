import random

import requests

from utils.wikidata import get_results


def resolve_get_property_gap_bounded_api_sandbox(entities):
    top_percentages = ["90%", "100%"]
    bot_percentages = ["10%", "20%"]
    ignore_percentages = ["30%", "40%", "50%", "60%", "70%", "80%"]
    top_entities = []
    bottom_entities = []
    counter = 0
    reversed_entities = entities[::-1]
    for entity in entities:
        counter += 1
        if entity["percentile"] in bot_percentages:
            bottom_entities.append(entity)
        elif entity["percentile"] in ignore_percentages:
            break
    for entity in reversed_entities:
        counter += 1
        if entity["percentile"] in top_percentages:
            top_entities.append(entity)
        elif entity["percentile"] in ignore_percentages:
            break
    top_properties = set()
    for entity in top_entities:
        for prop in entity["entityProperties"]:
            top_properties.add(prop)
    bot_properties = set()
    for entity in bottom_entities:
        for prop in entity["entityProperties"]:
            bot_properties.add(prop)

    result_properties = top_properties.difference(bot_properties)
    result_properties = list(result_properties)
    result = {"properties": result_properties}
    return result


def resolve_get_property_gap_unbounded_api_sandbox(entities):
    from resolver.resolver import LIMITS, ENDPOINT_URL
    top_percentages = ["90%", "100%"]
    bot_percentages = ["10%", "20%"]
    ignore_percentages = ["30%", "40%", "50%", "60%", "70%", "80%"]
    top_entities = []
    bottom_entities = []
    counter = 0
    reversed_entities = entities[::-1]
    for entity in entities:
        counter += 1
        if entity["percentile"] in bot_percentages:
            bottom_entities.append(entity)
        elif entity["percentile"] in ignore_percentages:
            break
    for entity in reversed_entities:
        counter += 1
        if entity["percentile"] in top_percentages:
            top_entities.append(entity)
        elif entity["percentile"] in ignore_percentages:
            break

    bot_gap = []
    if len(top_entities) > 50:
        top_random = random.sample(range(len(top_entities)), 50)
        for idx in top_random:
            bot_gap.append(top_entities[idx]["entity"])
    else:
        for entity in top_entities:
            bot_gap.append(entity["entity"])
    bot_entities_string = ""
    for elem in bot_gap:
        bot_entities_string += "{wd:%s ?property ?o .} UNION " % elem
    bot_entities_string = bot_entities_string[:-6]

    bot_query = """
    SELECT DISTINCT ?p {
    %s
    FILTER(CONTAINS(STR(?property),"http://www.wikidata.org/prop/direct/"))
      FILTER NOT EXISTS {?p wikibase:propertyType wikibase:ExternalId .}
      ?p wikibase:directClaim ?property .
    }
    """ % bot_entities_string
    query_results = get_results(ENDPOINT_URL, bot_query)
    property_results = query_results["results"]["bindings"]
    top_gap_set = set()
    for prop in property_results:
        property_link = prop["p"]["value"]
        property_id = property_link.split("/")[-1]
        top_gap_set.add(property_id)

    bot_gap = []
    if len(bottom_entities) > 50:
        bottom_random = random.sample(range(len(bottom_entities)), 50)
        for idx in bottom_random:
            bot_gap.append(bottom_entities[idx]["entity"])
    else:
        for entity in bottom_entities:
            bot_gap.append(entity["entity"])
    bot_entities_string = ""
    for elem in bot_gap:
        bot_entities_string += "{wd:%s ?property ?o .} UNION " % elem
    bot_entities_string = bot_entities_string[:-6]

    bot_query = """
        SELECT DISTINCT ?p {
        %s
        FILTER(CONTAINS(STR(?property),"http://www.wikidata.org/prop/direct/"))
          FILTER NOT EXISTS {?p wikibase:propertyType wikibase:ExternalId .}
          ?p wikibase:directClaim ?property .
        }
        """ % bot_entities_string
    query_results = get_results(ENDPOINT_URL, bot_query)
    property_results = query_results["results"]["bindings"]
    bot_gap_set = set()
    for prop in property_results:
        property_link = prop["p"]["value"]
        property_id = property_link.split("/")[-1]
        bot_gap_set.add(property_id)

    result_properties = list(top_gap_set.difference(bot_gap_set))
    result = {"properties": result_properties}
    return result
