from utils.wikidata import get_results


def resolve_get_analysis_information_result(single_dashboard, property_id):
    from resolver.resolver import LIMITS, ENDPOINT_URL
    entity_id = single_dashboard.entity
    filters = eval(single_dashboard.filters)
    filter_query = ""
    for elem in filters:
        for elem_filter in elem.keys():
            filter_query += "?entity wdt:%s wd:%s . " % (elem_filter, elem[elem_filter])
    query = """
    SELECT DISTINCT  ?value ?valueLabel
          WHERE {
            {SELECT ?value ?valueLabel
            WHERE {
              ?entity wdt:P31 wd:%s.
              %s
              ?entity wdt:%s ?value . 
              ?value rdfs:label ?valueLabel .
                FILTER(LANG(?valueLabel)="en")
            }
            LIMIT %s}
          }
    """ % (entity_id, filter_query, property_id, LIMITS["unbounded"])
    query_results = get_results(ENDPOINT_URL, query)
    values_query_results = query_results["results"]["bindings"]
    values_result = []
    for value in values_query_results:
        value_link = value["value"]["value"]
        value_id = value_link.split("/")[-1]
        value_label = value["valueLabel"]["value"]
        value_obj = {"valueLink": value_link, "value_id": value_id, "value_label": value_label}
        values_result.append(value_obj)

    result = {"amount": len(values_result), "propertyID": property_id, "values": values_result}
    return result
