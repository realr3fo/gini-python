import requests
from bs4 import BeautifulSoup


def resolve_get_entities_count_result(single_dashboard):
    entity = single_dashboard.entity
    count = 0
    wikidata_url = "https://www.wikidata.org/w/index.php?search=haswbstatement%%3AP31%%3D%s&title=Special%%3ASearch" \
                   % entity
    wikidata_result = requests.get(wikidata_url)
    soup = BeautifulSoup(wikidata_result.text, "html.parser")
    result = {"entityCount": count}
    return result
