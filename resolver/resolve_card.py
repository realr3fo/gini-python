import requests
from bs4 import BeautifulSoup

from utils.wikidata import get_results


def resolve_get_entities_count_result(single_dashboard):
    entity = single_dashboard.entity
    wikidata_url = "https://www.wikidata.org/w/index.php?search=haswbstatement%%3AP31%%3D%s&title=Special%%3ASearch" \
                   % entity
    wikidata_result = requests.get(wikidata_url)
    soup = BeautifulSoup(wikidata_result.text, "html.parser")
    strong_elem = soup.findAll('strong')
    entity_count = strong_elem[-1]
    entity_count_text = entity_count.text
    entity_count_text = ''.join(entity_count_text.split(","))
    entity_count_int = int(entity_count_text)
    count = entity_count_int
    result = {"entityCount": count}
    return result


def resolve_get_entities_same_as_profile(single_dashboard):
    from resolver.resolver import ENDPOINT_URL, LIMITS
    entity = single_dashboard.entity
    filters = eval(single_dashboard.filters)
    filter_query = ""
    for elem in filters:
        for elem_filter in elem.keys():
            filter_query += "?item wdt:%s wd:%s . " % (elem_filter, elem[elem_filter])
    query = """
    SELECT (count( ?item ) as ?cnt) WHERE {
  ?item wdt:P31 wd:%s .
  %s  
} LIMIT %d
    """ % (entity, filter_query, 10000)
    query_results = get_results(ENDPOINT_URL, query)
    query_results = query_results["results"]["bindings"][0]
    count = query_results["cnt"]["value"]
    result = {"entityCount": count}
    return result
