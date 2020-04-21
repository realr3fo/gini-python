import json

import requests

from resolver.resolver import ENDPOINT_URL
from utils.wikidata import get_results


def resolve_get_wikidata_entities_result(search):
    entities = []
    if search == "" or search is None:
        with open('./resolver/top_entities.json') as json_file:
            data = json.load(json_file)
            for entity in data['search']:
                entities.append(entity)
    else:
        search_url = "https://www.wikidata.org/w/api.php?action=wbsearchentities&format=json&origin=*&type=item" \
                     "&search=%s&language=en" % search
        search_result = requests.get(search_url)
        search_result = search_result.json()
        for entity in search_result["search"]:
            entities.append(entity)
    result = {"amount": len(entities), "entities": entities}
    return result


def resolve_get_filter_suggestions_result(entity_id, filled_properties):
    suggestions = []
    filters = ""
    if filled_properties != "" and filled_properties is not None:
        filled_property = filled_properties.split(",")
        for elem in filled_property:
            filters += " FILTER(?p != wdt:%s) " % elem
    query = """
    SELECT ?pFull ?pFullLabel ?cnt {
          ?pFull wikibase:directClaim ?p .
          MINUS {?pFull <http://wikiba.se/ontology#propertyType> <http://wikiba.se/ontology#ExternalId>}
          {
            SELECT ?p (COUNT(?s) AS ?cnt) {
             SELECT DISTINCT ?s ?p WHERE {
                {SELECT DISTINCT ?s {
                  { SELECT ?s WHERE {
                    ?s wdt:P31 wd:%s.
                  } LIMIT 1000 }
                }}
                OPTIONAL {
                  ?s ?p ?o .
                  FILTER(STRSTARTS(STR(?p),"http://www.wikidata.org/prop/direct/")) # only select direct statements
                }
               FILTER(?p != wdt:P31)
               FILTER(?p != wdt:P373)
               %s
              }
            } GROUP BY ?p
          }
          SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". } # get labels
        } ORDER BY DESC(?cnt)
        limit 10
    """ % (entity_id, filters)
    query_results = get_results(ENDPOINT_URL, query)
    results = query_results["results"]["bindings"]
    for elem in results:
        prop_link = elem["pFull"]["value"]
        prop_id = prop_link.split("/")[-1]
        prop_label = elem["pFullLabel"]["value"]
        prop_count = elem["cnt"]["value"]
        prop_obj = {"propertyLink": prop_link, "propertyID": prop_id, "propertyLabel": prop_label,
                    "propertyCount": prop_count}
        suggestions.append(prop_obj)
    result = {"amount": len(suggestions), "suggestions": suggestions}
    return result


def resolve_get_wikidata_properties_result(search):
    properties = []
    if search == "" or search is None:
        pass
    else:
        search_url = "https://www.wikidata.org/w/api.php?action=wbsearchentities&format=json&search=%s&language=en" \
                     "&type=property" % search
        search_result = requests.get(search_url)
        search_result = search_result.json()
        for entity in search_result["search"]:
            properties.append(entity)
    result = {"amount": len(properties), "properties": properties}
    return result
