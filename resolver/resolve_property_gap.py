import random

import requests


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

    top_gap = []
    if len(top_entities) > 50:
        top_random = random.sample(range(len(top_entities)), 50)
        for idx in top_random:
            top_gap.append(top_entities[idx]["entity"])
    else:
        for entity in top_entities:
            top_gap.append(entity["entity"])
    entities_string = ""
    for elem in top_gap:
        entities_string += elem + "%7C"
    entities_string = entities_string[:-3]
    endpoint = "http://wikidata.org/w/api.php?action=wbgetentities&format=json&ids=%s&props=claims&languages=en" % \
               entities_string
    print("before")
    print(endpoint)
    response = requests.get(endpoint)
    print("after")

    result_entities = response.json()["entities"]
    top_gap_set = set()
    for single_entity in result_entities.keys():
        single_entity_properties = result_entities[single_entity]["claims"]
        for prop in single_entity_properties.keys():
            top_gap_set.add(prop)
    bot_gap = []
    if len(bottom_entities) > 50:
        bottom_random = random.sample(range(len(bottom_entities)), 50)
        for idx in bottom_random:
            bot_gap.append(bottom_entities[idx]["entity"])
    else:
        for entity in bottom_entities:
            bot_gap.append(entity["entity"])
    entities_string = ""
    for elem in bot_gap:
        entities_string += elem + "%7C"
    entities_string = entities_string[:-3]
    endpoint = "http://wikidata.org/w/api.php?action=wbgetentities&format=json&ids=%s&props=claims&languages=en" % \
               entities_string
    response = requests.get(endpoint)
    result_entities = response.json()["entities"]
    bot_gap_set = set()
    for single_entity in result_entities.keys():
        single_entity_properties = result_entities[single_entity]["claims"]
        for prop in single_entity_properties.keys():
            bot_gap_set.add(prop)

    result_properties = list(top_gap_set.difference(bot_gap_set))

    result = {"properties": result_properties}
    return result
