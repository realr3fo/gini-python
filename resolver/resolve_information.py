import json
import asyncio

import requests

from utils.wikidata import get_results, async_get_results


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


def resolve_get_item_info_sandbox(item_id):
    wikidata_url = "https://www.wikidata.org/w/api.php?action=wbgetentities&format=json&ids=%s&props=labels" \
                   "%%7Cdescriptions&languages=en" % item_id
    wikidata_result = requests.get(wikidata_url)
    wikidata_result = wikidata_result.json()
    objects = wikidata_result["entities"]
    label = objects[item_id]["labels"]["en"]["value"]
    description = objects[item_id]["descriptions"]["en"]["value"]
    result = {"id": item_id, "label": label, "description": description}
    return result


def resolve_get_compare_filters_info_result(single_dashboard):
    compare_filters = eval(single_dashboard.compare_filters)
    compare_infos = []
    for compare_filter in compare_filters:
        prop = compare_filter['propertyID']
        item1 = compare_filter['value']['item1']
        item2 = compare_filter['value']['item2']
        prop_info = resolve_get_item_info_sandbox(prop)
        item_1_info = resolve_get_item_info_sandbox(item1)
        item_2_info = resolve_get_item_info_sandbox(item2)
        prop_info["value"] = {}
        prop_info["value"]["item1"] = item_1_info
        prop_info["value"]["item2"] = item_2_info
        compare_infos.append(prop_info)
    result = compare_infos
    return result


def resolve_get_analysis_properties_info_result(single_dashboard):
    analysis_properties = eval(single_dashboard.analysis_filters)
    analysis_infos = []
    for analysis_property in analysis_properties:
        prop_info = resolve_get_item_info_sandbox(analysis_property)
        analysis_infos.append(prop_info)
    result = analysis_infos
    return result


def resolve_get_properties_info_result(single_dashboard):
    entity_id = single_dashboard.entity
    entity_filters = eval(single_dashboard.filters)
    properties = eval(single_dashboard.properties)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    properties_result = loop.run_until_complete(get_properties_binding(entity_id, entity_filters, properties))
    loop.close()
    result = properties_result
    return result


def resolve_get_dashboard_info_result(single_dashboard):
    single_dashboard_data = single_dashboard.serialize()
    for key in single_dashboard_data.keys():
        try:
            single_dashboard_data[key] = eval(single_dashboard_data[key])
        except (TypeError, SyntaxError, NameError):
            continue
    return single_dashboard_data


def resolve_get_properties_info_compare_result(single_dashboard):
    entity_id = single_dashboard.entity
    entity_filters = eval(single_dashboard.filters)
    properties = eval(single_dashboard.properties)
    compare_filters = eval(single_dashboard.compare_filters)
    compare_filters_item_1 = []
    compare_filters_item_2 = []
    for item_filter in compare_filters:
        prop = item_filter["propertyID"]
        item_val_1 = item_filter["value"]["item1"]
        item_val_2 = item_filter["value"]["item2"]
        filter_obj_1 = {prop: item_val_1}
        filter_obj_2 = {prop: item_val_2}
        compare_filters_item_1.append(filter_obj_1)
        compare_filters_item_2.append(filter_obj_2)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    task_1 = loop.create_task(get_properties_binding(entity_id, entity_filters, properties, compare_filters_item_1))
    task_2 = loop.create_task(get_properties_binding(entity_id, entity_filters, properties, compare_filters_item_2))
    properties_result_1, properties_result_2 = loop.run_until_complete(asyncio.gather(task_1, task_2))
    prop_result = []
    properties_result_1 = properties_result_1["result"]
    properties_result_2 = properties_result_2["result"]
    for key in properties_result_1:
        prop_id = properties_result_1[key]['id']
        prop_label = properties_result_1[key]['label']
        prop_link = properties_result_1[key]['link']
        prop_1_count = properties_result_1[key]['count']
        if key in properties_result_2:
            prop_2_count = properties_result_2[key]['count']
        else:
            prop_2_count = "0"
        prop_obj = {"id": prop_id, "label": prop_label, "link": prop_link, "count1": prop_1_count,
                    "count2": prop_2_count}
        prop_result.append(prop_obj)

    for key in properties_result_2:
        prop_id = properties_result_2[key]['id']
        prop_label = properties_result_2[key]['label']
        prop_link = properties_result_2[key]['link']
        prop_2_count = properties_result_2[key]['count']
        if key in properties_result_1:
            pass
        else:
            prop_1_count = "0"
            prop_obj = {"id": prop_id, "label": prop_label, "link": prop_link, "count1": prop_1_count,
                        "count2": prop_2_count}
            prop_result.append(prop_obj)
    loop.close()
    result = {"result": prop_result}
    return result


async def get_properties_binding(entity_id, entity_filters, properties, compare_item_filters=None):
    properties_result = {"result": {}}
    filter_query_top = ""
    filter_query_bottom = ""
    for elem in entity_filters:
        for elem_filter in elem.keys():
            filter_query_top += "?s wdt:%s wd:%s . " % (elem_filter, elem[elem_filter])
            filter_query_bottom += "FILTER(?p != wdt:%s) " % elem_filter
    if compare_item_filters is not None:
        for elem in compare_item_filters:
            for elem_filter in elem.keys():
                filter_query_top += "?s wdt:%s wd:%s . " % (elem_filter, elem[elem_filter])
                filter_query_bottom += "FILTER(?p != wdt:%s) " % elem_filter
    filter_property = ""
    for elem in properties:
        filter_property += "FILTER(?p = wdt:%s)" % elem
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
              SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". } # get labels
        } ORDER BY DESC(?cnt)
            """ % (entity_id, filter_query_top, filter_query_bottom, filter_property)
    from resolver.resolver import ENDPOINT_URL
    query_results = await async_get_results(ENDPOINT_URL, query)
    properties_bindings = query_results["results"]["bindings"]
    for prop in properties_bindings:
        property_link = prop["pFull"]["value"]
        property_id = property_link.split("/")[-1]
        property_label = prop["pFullLabel"]["value"]
        property_entities_count = prop["cnt"]["value"]
        property_obj = {"id": property_id, "label": property_label,
                        "link": property_link,
                        "count": property_entities_count}
        properties_result["result"][property_id] = property_obj
    return properties_result


async def get_class_info_from_wikidata(entity_id):
    query_class = """
        select ?class ?classDescription ?classLabel{
      wd:%s wdt:P31 ?class .
      SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
    }
        """ % entity_id
    from resolver.resolver import ENDPOINT_URL
    query_results = await async_get_results(ENDPOINT_URL, query_class)
    query_results_bindings = query_results["results"]["bindings"]
    return query_results_bindings


async def get_prop_obj_info_from_wikidata(entity_id):
    query_prop_obj = """
        select ?prop ?propLabel ?propDescription ?o ?oLabel ?oDescription{
      wd:%s ?p ?o . FILTER(CONTAINS(STR(?p),"http://www.wikidata.org/prop/direct/"))  
      ?prop wikibase:directClaim ?p . FILTER NOT EXISTS {?prop wikibase:propertyType wikibase:ExternalId .}
      SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
    }
        """ % entity_id
    from resolver.resolver import ENDPOINT_URL
    query_results = await async_get_results(ENDPOINT_URL, query_prop_obj)
    query_results_bindings = query_results["results"]["bindings"]
    return query_results_bindings


def resolve_get_entity_info_result(entity_id):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    tasks = [loop.create_task(get_class_info_from_wikidata(entity_id)),
             loop.create_task(get_prop_obj_info_from_wikidata(entity_id))]
    # create instance of Semaphore
    query_results_arr = loop.run_until_complete(asyncio.gather(*tasks))
    loop.close()
    query_results_bindings = [sublist for sublist in query_results_arr]
    class_results = query_results_bindings[0]
    prop_obj_results = query_results_bindings[1]
    classes = []
    for elem in class_results:
        class_link = elem['class']['value']
        class_id = class_link.split("/")[-1]
        class_label = elem.get('classLabel', {}).get('value', '')
        class_description = elem.get('classDescription', {}).get('value', '')
        class_obj = {'id': class_id, 'label': class_label, 'description': class_description, 'link': class_link}
        classes.append(class_obj)
    filters = []
    for elem in prop_obj_results:
        prop_link = elem['prop']['value']
        prop_id = prop_link.split("/")[-1]
        prop_label = elem.get('propLabel', {}).get('value', '')
        prop_description = elem.get('propDescription', {}).get('value', '')
        obj_link = elem['o']['value']
        obj_id = obj_link.split("/")[-1]
        obj_label = elem.get('oLabel', {}).get('value', '')
        obj_description = elem.get('oDescription', {}).get('value', '')
        property_obj = {'id': prop_id, 'label': prop_label, 'description': prop_description, 'link': prop_link}
        object_obj = {'id': obj_id, 'label': obj_label, 'description': obj_description, 'link': obj_link}
        filter_obj = {'property': property_obj, 'value': object_obj}
        filters.append(filter_obj)
    result = {'classes': classes, 'filters': filters}
    return result
