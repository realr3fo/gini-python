#!venv/bin/python3
from flask import Flask, jsonify
from wikidata import get_results

app = Flask(__name__)


@app.route('/', methods=['GET'])
def get_tasks():
    endpoint_url = "https://query.wikidata.org/sparql"

    query = "select ?item {?item  wdt:P31 wd:Q808.}"
    results = get_results(endpoint_url, query)

    return jsonify(results)


if __name__ == '__main__':
    app.run()
