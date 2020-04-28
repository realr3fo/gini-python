import requests
from bs4 import BeautifulSoup


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
