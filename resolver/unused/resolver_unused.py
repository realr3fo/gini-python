from resolver.resolver import ENDPOINT_URL
from utils.wikidata import get_results


def resolve_get_property_value_suggestions(entity_id, property_id, filters):
    value_suggestions = []
    filter_query = ""
    if len(filters) != 0:
        for elem in filters:
            for elem_key in elem.keys():
                filter_query += " ?entity wdt:%s wd:%s . " % (elem_key, elem[elem_key])
    query = """
    SELECT DISTINCT ?value ?valueLabel
          WHERE {
            SELECT ?value ?valueLabel
            WHERE {
              ?entity wdt:P31 wd:%s.
              %s
              ?entity wdt:%s ?value.
                ?value rdfs:label ?valueLabel .
                FILTER(LANG(?valueLabel)="en")
            }
            LIMIT 10000
          }
    """ % (entity_id, filter_query, property_id)
    query_results = get_results(ENDPOINT_URL, query)
    query_bindings = query_results["results"]["bindings"]
    for elem in query_bindings:
        value_link = elem['value']['value']
        value_label = elem["valueLabel"]['value']
        value_id = value_link.split("/")[-1]
        value_obj = {"entityID": value_id, "entityLabel": value_label, "entityLink": value_link}
        value_suggestions.append(value_obj)
    result = {"amount": len(value_suggestions), "suggestions": value_suggestions}
    return result
