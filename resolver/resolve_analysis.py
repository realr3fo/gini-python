import asyncio
import itertools

from utils.gini import calculate_gini, get_chunked_arr, get_cumulative_data_and_entities, normalize_data
from utils.wikidata import get_results, async_get_results


async def get_property_values(entity_id, filter_query, property_id):
    from resolver.resolver import LIMITS, ENDPOINT_URL
    query = """
        SELECT DISTINCT  ?value
              WHERE {
                {SELECT ?value
                WHERE {
                  ?entity wdt:P31 wd:%s.
                  %s
                  ?entity wdt:%s ?value . 
                }
                LIMIT %s}
              }
        """ % (entity_id, filter_query, property_id, LIMITS["unbounded"])
    query_results = await async_get_results(ENDPOINT_URL, query)
    result = {"property_id": property_id, "values": query_results["results"]["bindings"]}
    return result


def resolve_get_analysis_information_result(single_dashboard):
    entity_id = single_dashboard.entity
    filters = eval(single_dashboard.filters)
    analysis_properties = eval(single_dashboard.analysis_filters)
    filter_query = ""
    for elem in filters:
        for elem_filter in elem.keys():
            filter_query += "?entity wdt:%s wd:%s . " % (elem_filter, elem[elem_filter])
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    tasks = []
    # create instance of Semaphore
    for property_id in analysis_properties:
        task = loop.create_task(get_property_values(entity_id, filter_query, property_id))
        tasks.append(task)
    query_results_arr = loop.run_until_complete(asyncio.gather(*tasks))
    loop.close()
    # query_results_bindings = [item for sublist in query_results_arr for item in sublist]
    # values_query_results = query_results_bindings

    results = {}
    for elem in query_results_arr:
        values_result = []
        property_id = elem["property_id"]
        values_query_results = [item for item in elem["values"]]
        for value in values_query_results:
            value_link = value["value"]["value"]
            value_id = value_link.split("/")[-1]
            # value_label = value["valueLabel"]["value"]
            value_label = ""
            value_obj = {"valueLink": value_link, "value_id": value_id, "value_label": value_label,
                         "property": property_id}
            values_result.append(value_obj)
        results[property_id] = values_result
    if len(analysis_properties) == 1:
        combinations = []
        for elem in results[analysis_properties[0]]:
            obj = {"item1": elem}
            combinations.append(obj)
        results = {"combinations": combinations}
    elif len(analysis_properties) == 2:
        combinations = []
        products = list(itertools.product(results[analysis_properties[0]], results[analysis_properties[1]]))
        for product in products:
            obj = {"item1": product[0], "item2": product[1]}
            combinations.append(obj)
        results = {"combinations": combinations}
    else:
        results = {}
    return results


def resolve_get_gini_analysis_result(single_dashboard, property_1, entity_1, property_2, entity_2):
    from resolver.resolver import LIMITS, ENDPOINT_URL
    from resolver.resolver_gini import get_insight, get_ten_percentile
    entity_id = single_dashboard.entity
    filters = eval(single_dashboard.filters)
    filter_query = ""
    for elem in filters:
        for elem_filter in elem.keys():
            filter_query += "?item wdt:%s wd:%s . " % (elem_filter, elem[elem_filter])
    filter_query += " ?item wdt:%s wd:%s . " % (property_1, entity_1)
    if property_2 != 0:
        filter_query += " ?item wdt:%s wd:%s . " % (property_2, entity_2)
    query = """
                SELECT ?item ?itemLabel ?cnt {
                    {SELECT ?item (COUNT(DISTINCT(?prop)) AS ?cnt) {

                    {SELECT DISTINCT ?item WHERE {
                       ?item wdt:P31 wd:%s . 
                       %s 
                    } LIMIT %d}
                    OPTIONAL { ?item ?p ?o . FILTER(CONTAINS(STR(?p),"http://www.wikidata.org/prop/direct/")) 
                    ?prop wikibase:directClaim ?p . FILTER NOT EXISTS {?prop wikibase:propertyType wikibase:ExternalId .} }

                    } GROUP BY ?item}

                    SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }

                    } ORDER BY DESC(?cnt)
                """ % (entity_id, filter_query, LIMITS["unbounded"])
    query_results = get_results(ENDPOINT_URL, query)
    item_arr = query_results["results"]["bindings"]
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

    q_arr = sorted(q_arr, key=lambda x: x[1])
    gini_coefficient = calculate_gini(q_arr)
    gini_coefficient = round(gini_coefficient, 3)
    if len(q_arr) >= LIMITS["unbounded"]:
        exceed_limit = True
    else:
        exceed_limit = False

    chunked_q_arr = get_chunked_arr(q_arr)
    each_amount = []
    for arr in chunked_q_arr:
        each_amount.append(len(arr))
    cumulative_data, entities = get_cumulative_data_and_entities(chunked_q_arr)
    original_data = list(cumulative_data)
    cumulative_data.insert(0, 0)
    data = normalize_data(cumulative_data)
    insight = get_insight(original_data)
    percentiles = get_ten_percentile(original_data)
    percentiles.insert(0, '0%')
    result = {"limit": LIMITS, "amount": sum(each_amount), "gini": gini_coefficient,
              "each_amount": each_amount,
              "data": data, "exceedLimit": exceed_limit, "percentileData": percentiles,
              "insight": insight, "entities": entities}
    return result


def resolve_get_property_analysis_result(single_dashboard, property_id, entity_analysis_id):
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
    filter_query_top += " ?s wdt:%s wd:%s . " % (property_id, entity_analysis_id)
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
