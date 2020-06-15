import asyncio
import itertools

from utils.gini import calculate_gini, get_chunked_arr, get_cumulative_data_and_entities, normalize_data, get_insight, \
    get_ten_percentile, get_new_histogram_data
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


def get_analysis_statistics(property_count_arr):
    sum_property_count = sum(property_count_arr)
    from statistics import mean
    average = round(mean(property_count_arr), 3)
    max_prop = max(property_count_arr)
    min_prop = min(property_count_arr)

    result = {"total_properties": sum_property_count, "average_distinct_properties": average, "min": min_prop,
              "max": max_prop}
    return result


async def get_gini_analysis_from_wikidata(object_ids, object_labels, filter_query, entity_id, offset_count):
    from resolver.resolver import LIMITS, ENDPOINT_URL
    limit = 1000
    offset = offset_count * 1000
    query = ("SELECT ?item ?itemLabel ?cnt {object_ids} {object_labels} {{\n"
             "  {{SELECT ?item {object_ids} (COUNT(DISTINCT(?prop)) AS ?cnt) {{\n"
             "\n"
             "    {{SELECT DISTINCT ?item {object_ids} WHERE {{\n"
             "      ?item wdt:P31 wd:{entity_id} . \n"
             "      {filter_query}\n"
             "    }} LIMIT {row_limit} OFFSET {offset} }}\n"
             "    OPTIONAL {{ ?item ?p ?o . FILTER(CONTAINS(STR(?p),\"http://www.wikidata.org/prop/direct/\")) \n"
             "              ?prop wikibase:directClaim ?p . FILTER NOT EXISTS {{?prop wikibase:propertyType wikibase:ExternalId .}} }}\n"
             "\n"
             "  }} GROUP BY ?item {object_ids} }}\n"
             "\n"
             "  SERVICE wikibase:label {{ bd:serviceParam wikibase:language \"[AUTO_LANGUAGE],en\". }}\n"
             "\n"
             "}} ORDER BY DESC(?cnt)\n").format(object_ids=object_ids, object_labels=object_labels, entity_id=entity_id,
                                                filter_query=filter_query, row_limit=limit, offset=offset)
    query_results = await async_get_results(ENDPOINT_URL, query)
    item_arr = query_results["results"]["bindings"]
    return item_arr


def resolve_get_gini_analysis_result(single_dashboard, shown_combinations, filter_limit):
    from resolver.resolver import LIMITS, ENDPOINT_URL
    entity_id = single_dashboard.entity
    filters = eval(single_dashboard.filters)
    analysis_filters = eval(single_dashboard.analysis_filters)
    analysis_info = single_dashboard.analysis_info
    filter_query = ""
    for combination in filters:
        for elem_filter in combination.keys():
            filter_query += "?item wdt:%s wd:%s . " % (elem_filter, combination[elem_filter])
    obj_labels = ""
    obj_ids = ""
    obj_ids_arr = []
    for combination in analysis_filters:
        filter_query += "?item wdt:%s ?o%s . " % (combination, combination)
        obj_labels += "?o%sLabel " % combination
        obj_ids += "?o%s " % combination
        obj_ids_arr.append("o%s" % combination)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    tasks = []
    # create instance of Semaphore
    for i in range(10):
        task = loop.create_task(get_gini_analysis_from_wikidata(obj_ids, obj_labels, filter_query, entity_id, i))
        tasks.append(task)
    query_results_arr = loop.run_until_complete(asyncio.gather(*tasks))
    loop.close()
    query_results_bindings = [item for sublist in query_results_arr for item in sublist]
    item_arr = query_results_bindings
    if len(item_arr) == 0:
        result = {"gini": 0, "data": [], "entities": []}
        return result
    q_arr = []
    obj_values = {}
    objects_label = {}
    for obj_id in obj_ids_arr:
        obj_values[obj_id] = set()
    total_amount = len(item_arr)
    for combination in item_arr:
        item_link = combination['item']['value']
        item_id = item_link.split("/")[-1]
        property_count = int(combination['cnt']['value'])
        item_label = combination['itemLabel']['value']
        entity_obj = (item_id, property_count, item_label, item_link)
        for obj_id in obj_ids_arr:
            entity_list = list(entity_obj)
            obj_link = combination[obj_id]['value']
            obj_value = obj_link.split("/")[-1]
            obj_label = combination[obj_id + "Label"]['value']
            obj_values[obj_id].add(obj_value)
            objects_label[obj_value] = obj_label
            entity_list.append(obj_value)
            entity_list.append(obj_label)
            entity_obj = tuple(entity_list)
        q_arr.append(entity_obj)
    for obj_id in obj_ids_arr:
        obj_values[obj_id] = list(obj_values[obj_id])
    combinations = []
    if len(obj_ids_arr) == 1:
        for product in obj_values[obj_ids_arr[0]]:
            obj = {"item_1": product, "shown": True}
            combinations.append(obj)
    if len(obj_ids_arr) == 2:
        products = list(itertools.product(obj_values[obj_ids_arr[0]], obj_values[obj_ids_arr[1]]))
        for product in products:
            obj = {"item_1": product[0], "item_2": product[1], "shown": True}
            combinations.append(obj)

    analysis_results = []
    max_number_of_items = 0
    for combination in combinations:
        new_q_arr = []
        obj_1_label = objects_label[combination["item_1"]]
        obj_2_label = ""
        for q in q_arr:
            if "item_2" in combination:
                obj_2_label = objects_label[combination["item_2"]]
                if q[4] == combination["item_1"] and q[6] == combination["item_2"]:
                    new_q_arr.append(q)
            else:
                if q[4] == combination["item_1"]:
                    new_q_arr.append(q)
        single_analysis_info = {
            "property_1": analysis_info[0]["id"],
            "propety_1_label": analysis_info[0]["label"],
            "item_1": combination["item_1"],
            "item_1_label": obj_1_label}
        if "item_2" in combination:
            single_analysis_info["property_2"] = analysis_info[1]["id"]
            single_analysis_info["property_2_label"] = analysis_info[1]["label"]
            single_analysis_info["item_2"] = combination["item_2"]
            single_analysis_info["item_2_label"] = obj_2_label

        new_q_arr = sorted(new_q_arr, key=lambda x: x[1])
        if len(new_q_arr) == 0:
            result = {"analysis_info": single_analysis_info, "statistics": {}, "limit": LIMITS,
                      "amount": 0, "histogram_data": [],
                      "newHistogramData": {},
                      "gini": 0,
                      "each_amount": [],
                      "data": [], "exceedLimit": False, "percentileData": [],
                      "insight": ""}
            analysis_results.append(result)
            continue

        gini_coefficient = calculate_gini(new_q_arr)
        gini_coefficient = round(gini_coefficient, 3)
        if len(new_q_arr) >= LIMITS["unbounded"]:
            exceed_limit = True
        else:
            exceed_limit = False
        chunked_q_arr = get_chunked_arr(new_q_arr)
        each_amount = []
        count = 0
        for arr in chunked_q_arr:
            count += len(arr)
            each_amount.append(count)
        cumulative_data, entities = get_cumulative_data_and_entities(chunked_q_arr)

        from collections import Counter
        property_counts = Counter(item['propertyCount'] for item in entities if item.get('propertyCount'))
        histogram_data = [count for _, count in property_counts.items()]
        if len(histogram_data) > 11:
            chunked_histogram_arr = get_chunked_arr(histogram_data, 11)
            histogram_data = []
            for elem in chunked_histogram_arr:
                histogram_data.append(sum(elem))
        from utils.utils import interpolated
        histogram_data = interpolated(histogram_data, 11)

        original_data = list(cumulative_data)
        cumulative_data.insert(0, 0)
        data = normalize_data(cumulative_data)
        insight = get_insight(original_data)
        percentiles = get_ten_percentile(original_data)
        percentiles.insert(0, '0%')

        property_count_arr = [x[1] for x in new_q_arr]
        statistics = get_analysis_statistics(property_count_arr)

        new_histogram_data = get_new_histogram_data(new_q_arr)

        amount = each_amount[-1]
        if amount > max_number_of_items:
            max_number_of_items = amount

        result = {"analysis_info": single_analysis_info, "statistics": statistics, "limit": LIMITS,
                  "amount": each_amount[-1], "histogram_data": histogram_data, "newHistogramData": new_histogram_data,
                  "gini": gini_coefficient,
                  "each_amount": each_amount,
                  "data": data, "exceedLimit": exceed_limit, "percentileData": percentiles,
                  "insight": insight}
        analysis_results.append(result)
    result = {"total_entities_amount": total_amount, "max_number": max_number_of_items, "combinations": combinations,
              "data": analysis_results}
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
