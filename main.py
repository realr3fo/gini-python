#!venv/bin/python3
import http
import json
import os
from dotenv import load_dotenv

from flask import Flask, request, abort
from flask_cors import CORS, cross_origin
from flask_sqlalchemy import SQLAlchemy

load_dotenv()

app = Flask(__name__)
cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'
app.config.from_object(os.environ['APP_SETTINGS'])
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

from resolver.resolver import *


@app.route('/', methods=['GET'])
@cross_origin()
def welcome():
    return "Welcome!"


@app.route('/api/entity/gini', methods=['GET', 'POST'])
@cross_origin()
def get_gini_with_filters():
    if request.method == 'GET':
        hash_code = request.args.get('hash_code')
        if hash_code == "" or hash_code is None:
            abort(http.HTTPStatus.INTERNAL_SERVER_ERROR, "Please include hash code")
        result = resolve_get_entity_gini_by_hash(hash_code)
        if "errorMessage" in result:
            abort(http.HTTPStatus.INTERNAL_SERVER_ERROR, result["errorMessage"])
        return json.dumps(result)
    elif request.method == 'POST':
        body = request.json
        if 'entity' not in body or 'filters' not in body:
            abort(http.HTTPStatus.INTERNAL_SERVER_ERROR, "Invalid Body")
        entity = body['entity']
        filters = body['filters']
        result = resolve_gini_with_filters_unbounded(entity, filters)
        return json.dumps(result)


@app.route('/api/entities', methods=['GET'])
@cross_origin()
def get_wikidata_entities():
    search = request.args.get('search')
    result = resolve_get_wikidata_entities(search)
    return json.dumps(result)


@app.route('/api/properties', methods=['GET'])
@cross_origin()
def get_wikidata_properties():
    search = request.args.get('search')
    result = resolve_get_wikidata_properties(search)
    return json.dumps(result)


@app.route('/api/filter/suggestions', methods=['GET'])
@cross_origin()
def get_filter_suggestions():
    entity_id = request.args.get('entity_id')
    if entity_id == "":
        abort(http.HTTPStatus.INTERNAL_SERVER_ERROR, "Please include entity id")
    filled_properties = request.args.get('filled_properties')
    result = resolve_get_filter_suggestions(entity_id, filled_properties)
    return json.dumps(result)


@app.route('/api/dashboard', methods=['POST'])
@cross_origin()
def create_dashboard():
    body = request.json
    if body is None or "entityID" not in body:
        abort(http.HTTPStatus.INTERNAL_SERVER_ERROR, "Please include entity id")
    entity_id = body['entityID']
    filters = []
    if "filters" in body:
        filters = body["filters"]

    result = resolve_create_dashboard(entity_id, filters)
    if "errorMessage" in result:
        abort(http.HTTPStatus.INTERNAL_SERVER_ERROR, result["errorMessage"])
    return json.dumps(result)


@app.route('/api/entity/information', methods=['GET'])
@cross_origin()
def get_entity_information():
    hash_code = request.args.get("hash_code")
    if hash_code == "" or hash_code is None:
        abort(http.HTTPStatus.INTERNAL_SERVER_ERROR, "Please include dashboard hashcode")
    result = resolve_get_entity_information(hash_code)
    return json.dumps(result)


@app.route('/api/entity/gini', methods=['GET'])
@cross_origin()
def get_entity_gini_by_hash():
    hash_code = request.args.get("hash_code")
    if hash_code == "" or hash_code is None:
        abort(http.HTTPStatus.INTERNAL_SERVER_ERROR, "Please include dashboard hashcode")
    result = resolve_get_entity_gini_by_hash(hash_code)
    return json.dumps(result)


@app.route('/api/properties/info', methods=['GET'])
@cross_origin()
def get_properties_info():
    hash_code = request.args.get("hash_code")
    if hash_code == "" or hash_code is None:
        abort(http.HTTPStatus.INTERNAL_SERVER_ERROR, "Please include dashboard hashcode")
    result = resolve_get_properties_info(hash_code)
    return json.dumps(result)


@app.route('/api/properties/gap', methods=['GET'])
@cross_origin()
def get_properties_gap():
    hash_code = request.args.get("hash_code")
    if hash_code == "" or hash_code is None:
        abort(http.HTTPStatus.INTERNAL_SERVER_ERROR, "Please include dashboard hashcode")
    result = resolve_get_property_gap_api_sandbox(hash_code)
    if "errorMessage" in result:
        abort(http.HTTPStatus.INTERNAL_SERVER_ERROR, result["errorMessage"])
    return json.dumps(result)


@app.route('/api/test/json', methods=['GET'])
@cross_origin()
def test_json():
    result = resolve_test_json()
    return json.dumps(result)


if __name__ == '__main__':
    app.run(debug=True)
