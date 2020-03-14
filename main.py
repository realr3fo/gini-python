#!venv/bin/python3
import http
import json

from flask import Flask, request, abort

from gini import calculate_gini, normalize_data, get_chunked_arr, get_cumulative_data_and_entities
from wikidata import get_results

app = Flask(__name__)


@app.route('/', methods=['GET'])
def welcome():
    return "Welcome!"


@app.route('/api/gini', methods=['GET'])
def get_gini():
    entity = request.args.get('entity')
    if entity is None or entity == "":
        abort(http.HTTPStatus.INTERNAL_SERVER_ERROR, "Please input the WikiData entity ID in query parameters")

    endpoint_url = "https://query.wikidata.org/sparql"

    query = """SELECT ?item ?itemLabel (SAMPLE(?image) AS ?image){?item wdt:P31 wd:%s. OPTIONAL { ?item wdt:P18 ?image}FILTER(STRSTARTS(STR(wdt:P18),"http://www.wikidata.org/prop/direct/"))SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }} GROUP BY ?item ?itemLabel""" % entity
    query_results = get_results(endpoint_url, query)
    item_arr = query_results["results"]["bindings"]
    if len(item_arr) == 0:
        abort(http.HTTPStatus.INTERNAL_SERVER_ERROR, "Result is empty row")
    q_arr = []
    for elem in item_arr:
        item_value = elem["item"]["value"]
        item_label = elem["itemLabel"]["value"]
        item_image = None
        if "image" in elem:
            item_image = elem["image"]["value"]
        q_value = item_value.split("/")[-1]
        query = """SELECT (COUNT(DISTINCT(?p)) AS ?propertyCount) {wd:%s ?p ?o .FILTER(STRSTARTS(STR(?p),
        "http://www.wikidata.org/prop/direct/"))}""" % q_value
        query_results = get_results(endpoint_url, query)
        property_count = int(query_results["results"]["bindings"][0]["propertyCount"]["value"])
        q_arr.append((q_value, property_count, item_label, item_image))

    q_arr = sorted(q_arr, key=lambda x: x[1])

    gini_coefficient = calculate_gini(q_arr)
    chunked_q_arr = get_chunked_arr(q_arr)
    cumulative_data, entities = get_cumulative_data_and_entities(chunked_q_arr)
    data = normalize_data(cumulative_data)

    result = {"gini": gini_coefficient, "data": data, "entities": entities}

    return json.dumps(result)


if __name__ == '__main__':
    app.run(debug=True)
