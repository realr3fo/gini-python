from utils.gini import calculate_gini, get_chunked_arr, get_cumulative_data_and_entities, normalize_data
from utils.wikidata import get_results


def resolve_get_comparison_gini_unbounded(data):
    from resolver.resolver import LIMITS, ENDPOINT_URL
    from resolver.resolver_gini import get_insight, get_ten_percentile
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
              "insight": insight}
    return result


def resolve_get_comparison_gini_bounded(data):
    from resolver.resolver_gini import get_insight, get_ten_percentile, get_each_amount_bounded
    from resolver.resolver import LIMITS, ENDPOINT_URL
    entity_id = data["entity_id"]
    filters = data["filters"]
    compare_filters = data["compare_filters"]
    item_number = "item" + str(data["item_number"])
    properties = data['properties']
    new_properties = []
    for elem in properties:
        new_elem = elem.strip()
        new_properties.append(new_elem)
    properties = new_properties

    jml_join = " + ?".join(properties)

    filter_query = ""
    for elem in filters:
        for elem_filter in elem.keys():
            filter_query += "?item wdt:%s wd:%s . " % (elem_filter, elem[elem_filter])
    for elem in compare_filters:
        elem_filter = elem["propertyID"]
        elem_value = elem["value"][item_number]
        filter_query += "?item wdt:%s wd:%s . " % (elem_filter, elem_value)

    query = "SELECT DISTINCT ?item ?itemLabel "
    for elem in properties:
        query += "?%s " % elem
    query += "(?%s AS ?count) {" % jml_join
    query += "{ SELECT ?item "
    for elem in properties:
        query += "?%s " % elem
    query += "WHERE{ { SELECT ?item WHERE { ?item wdt:P31 wd:%s . %s } LIMIT %d}" % (
        entity_id, filter_query, LIMITS["bounded"])
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
        entity_props = []
        for prop in properties:
            if elem[prop]["value"] == "1":
                entity_props.append(prop)
        item_label = elem["itemLabel"]["value"]
        entity_obj = (item_id, property_count, item_label, item_link, entity_props)
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
    original_data = list(cumulative_data)
    cumulative_data.insert(0, 0)
    data = normalize_data(cumulative_data)
    insight = get_insight(original_data)
    percentiles = get_ten_percentile(original_data)
    percentiles.insert(0, '0%')
    # property_gap = get_property_gap(chunked_q_arr)
    result = {"insight": insight, "limit": LIMITS, "amount": sum(each_amount),
              "gini": gini_coefficient, "each_amount": each_amount, "exceedLimit": exceed_limit,
              "percentileData": percentiles,
              "data": data}
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
    if len(properties) == 0:
        result = resolve_get_comparison_gini_unbounded(data)
    else:
        result = resolve_get_comparison_gini_bounded(data)
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
