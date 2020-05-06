import json

import requests

from utils.wikidata import get_results


def resolve_get_entity_information_result(single_dashboard):
    dashboard_name = single_dashboard.name
    if dashboard_name == "":
        dashboard_name = "Untitled Dashboard"
    dashboard_author = single_dashboard.author
    if dashboard_author == "":
        dashboard_author = "Anonymous"
    entity_id = single_dashboard.entity
    filters = eval(single_dashboard.filters)
    properties = eval(single_dashboard.properties)
    ids = ""
    ids += str(entity_id)
    for single_filter in filters:
        for property_id, entity_id in single_filter.items():
            ids += "%7C" + property_id
            ids += "%7C" + entity_id
    for single_property in properties:
        ids += "%7C" + single_property

    wikidata_url = "https://www.wikidata.org/w/api.php?action=wbgetentities&format=json&ids=%s&props=labels" \
                   "%%7Cdescriptions&languages=en" % ids
    wikidata_result = requests.get(wikidata_url)
    wikidata_result = wikidata_result.json()
    objects = wikidata_result["entities"]

    entity_id = single_dashboard.entity
    entity_label = objects[entity_id]["labels"]["en"]["value"]
    entity_description = objects[entity_id]["descriptions"]["en"]["value"]
    result_entity = {"entityID": entity_id, "entityLabel": entity_label, "entityDescription": entity_description}

    result_filters = []
    for single_filter in filters:
        for property_id, entity_id in single_filter.items():
            filter_id = property_id
            filter_label = objects[filter_id]["labels"]["en"]["value"]
            filter_description = objects[filter_id]["descriptions"]["en"]["value"]

            filter_value_id = entity_id
            filter_value_label = objects[filter_value_id]["labels"]["en"]["value"]
            filter_value_description = objects[filter_value_id]["descriptions"]["en"]["value"]

            result_filter_object = {"filterID": filter_id, "filterLabel": filter_label,
                                    "filterDescription": filter_description, "filterValueID": filter_value_id,
                                    "filterValueLabel": filter_value_label,
                                    "filterValueDescription": filter_value_description}
            result_filters.append(result_filter_object)

    result_properties = []
    for single_property in properties:
        property_id = single_property
        property_label = objects[property_id]["labels"]["en"]["value"]
        property_description = objects[property_id]["descriptions"]["en"]["value"]
        property_object = {"propertyID": property_id, "propertyLabel": property_label,
                           "propertyDescription": property_description}
        result_properties.append(property_object)

    result = {"name": dashboard_name, "author": dashboard_author, "entity": result_entity, "filters": result_filters,
              "properties": result_properties}
    return result


def resolve_get_properties_info_result(single_dashboard):
    entity_id = single_dashboard.entity
    entity_filters = eval(single_dashboard.filters)
    properties = eval(single_dashboard.properties)
    properties_result = []

    filter_query_top = ""
    filter_query_bottom = ""
    for elem in entity_filters:
        for elem_filter in elem.keys():
            filter_query_top += "?s wdt:%s wd:%s . " % (elem_filter, elem[elem_filter])
            filter_query_bottom += "FILTER(?p != wdt:%s) " % elem_filter
    filter_property = ""
    for elem in properties:
        filter_property += "FILTER(?p = wdt:%s)" % elem
    query = """
    SELECT ?pFull ?pFullLabel ?pDescription ?cnt {
      ?pFull wikibase:directClaim ?p .
      MINUS {?pFull <http://wikiba.se/ontology#propertyType> <http://wikiba.se/ontology#ExternalId>}
      {
        SELECT ?p (COUNT(?s) AS ?cnt) {
         SELECT DISTINCT ?s ?p WHERE {
            {SELECT DISTINCT ?s {
              { SELECT ?s WHERE {
                ?s wdt:P31 wd:%s.
                %s
              } LIMIT 10000 }
            }}
            OPTIONAL {
              ?s ?p ?o .
              FILTER(STRSTARTS(STR(?p),"http://www.wikidata.org/prop/direct/")) # only select direct statements
            }
           FILTER(?p != wdt:P31)
           FILTER(?p != wdt:P373)
           %s
           %s
          }
        } GROUP BY ?p
      }
      ?pFull  schema:description ?pDescription.
      FILTER(LANG(?pDescription)="en")
      SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". } # get labels
} ORDER BY DESC(?cnt)
    """ % (entity_id, filter_query_top, filter_query_bottom, filter_property)
    from resolver.resolver import ENDPOINT_URL
    query_results = get_results(ENDPOINT_URL, query)
    properties_bindings = query_results["results"]["bindings"]
    for prop in properties_bindings:
        property_link = prop["pFull"]["value"]
        property_id = property_link.split("/")[-1]
        property_label = prop["pFullLabel"]["value"]
        property_description = prop["pDescription"]["value"]
        property_entities_count = prop["cnt"]["value"]
        property_obj = {"propertyID": property_id, "propertyLabel": property_label,
                        "propertyDescription": property_description, "propertyLink": property_link,
                        "entitiesCount": property_entities_count}
        properties_result.append(property_obj)

    result = {"properties": properties_result}
    return result


def resolve_get_dashboard_info_result(single_dashboard):
    single_dashboard_data = single_dashboard.serialize()
    del single_dashboard_data["instances"]
    for key in single_dashboard_data.keys():
        try:
            single_dashboard_data[key] = eval(single_dashboard_data[key])
        except (TypeError, SyntaxError, NameError):
            continue
    return single_dashboard_data
