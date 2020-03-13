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

    query = "select DISTINCT ?item {?item  wdt:P31 wd:%s.} LIMIT 500" % entity
    query_results = get_results(endpoint_url, query)
    item_arr = query_results["results"]["bindings"]
    q_arr = []
    for elem in item_arr:
        item_value = elem["item"]["value"]
        q_value = item_value.split("/")[-1]
        query = """SELECT (COUNT(DISTINCT(?p)) AS ?propertyCount) {wd:%s ?p ?o .FILTER(STRSTARTS(STR(?p),
        "http://www.wikidata.org/prop/direct/"))}""" % q_value
        query_results = get_results(endpoint_url, query)
        property_count = int(query_results["results"]["bindings"][0]["propertyCount"]["value"])
        q_arr.append((q_value, property_count))

    q_arr = sorted(q_arr, key=lambda x: x[1])

    gini_coefficient = calculate_gini(q_arr)
    chunked_q_arr = get_chunked_arr(q_arr)
    cumulative_data, entities = get_cumulative_data_and_entities(chunked_q_arr)
    data = normalize_data(cumulative_data)

    result = {"gini": gini_coefficient, "data": data, "entities": entities}

    return json.dumps(result)


if __name__ == '__main__':
    app.run(debug=True)
