import asyncio

from utils.gini import construct_results_gini
from utils.wikidata import async_get_results, get_results


async def get_gini_from_wikidata(entity, filter_query, offset_count):
    from resolver.resolver import ENDPOINT_URL
    limit = 1000
    offset = offset_count * 1000
    query = """
                SELECT ?item ?itemLabel ?cnt {
                    {SELECT ?item (COUNT(DISTINCT(?prop)) AS ?cnt) {

                    {SELECT DISTINCT ?item WHERE {
                       ?item wdt:P31 wd:%s . 
                       %s 
                    } LIMIT %d offset %d}
                    OPTIONAL { ?item ?p ?o . FILTER(CONTAINS(STR(?p),"http://www.wikidata.org/prop/direct/")) 
                    ?prop wikibase:directClaim ?p . FILTER NOT EXISTS {?prop wikibase:propertyType wikibase:ExternalId .} }

                    } GROUP BY ?item}

                    SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }

                    } ORDER BY DESC(?cnt)
                """ % (entity, filter_query, limit, offset)
    query_results = await async_get_results(ENDPOINT_URL, query)
    return query_results["results"]["bindings"]


def resolve_get_comparison_gini_unbounded(data):
    entity_id = data["entity_id"]
    filters = data["filters"]
    compare_filters = data["compare_filters"]
    item_number = "item" + str(data["item_number"])
    filter_query = ""
    for elem in filters:
        for elem_filter in elem.keys():
            filter_query += "?item wdt:%s wd:%s . " % (elem_filter, elem[elem_filter])
    for elem in compare_filters:
        elem_filter = elem["propertyID"]
        elem_value = elem["value"][item_number]
        filter_query += "?item wdt:%s wd:%s . " % (elem_filter, elem_value)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    tasks = []
    # create instance of Semaphore
    for i in range(10):
        task = loop.create_task(get_gini_from_wikidata(entity_id, filter_query, i))
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
        entity_obj = (item_id, property_count, item_label, item_link)
        q_arr.append(entity_obj)
    result = construct_results_gini(q_arr)
    return result


def resolve_get_comparison_gini_result(single_dashboard, item_number):
    entity_id = single_dashboard.entity
    filters = eval(single_dashboard.filters)
    properties = eval(single_dashboard.properties)
    compare_filters = eval(single_dashboard.compare_filters)
    data = {
        "entity_id": entity_id,
        "filters": filters,
        "properties": properties,
        "compare_filters": compare_filters,
        "item_number": item_number
    }
    result = resolve_get_comparison_gini_unbounded(data)
    return result


def resolve_get_comparison_properties_result(single_dashboard, item_number):
    entity_id = single_dashboard.entity
    entity_filters = eval(single_dashboard.filters)
    properties = eval(single_dashboard.properties)
    compare_filters = eval(single_dashboard.compare_filters)
    properties_result = []

    filter_query_top = ""
    filter_query_bottom = ""
    for elem in entity_filters:
        for elem_filter in elem.keys():
            filter_query_top += "?s wdt:%s wd:%s . " % (elem_filter, elem[elem_filter])
            filter_query_bottom += "FILTER(?p != wdt:%s) " % elem_filter
    for elem in compare_filters:
        elem_filter = elem["propertyID"]
        elem_value = elem["value"][item_number]
        filter_query_top += "?item wdt:%s wd:%s . " % (elem_filter, elem_value)
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
