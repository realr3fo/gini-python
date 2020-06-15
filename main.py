#!venv/bin/python3
import http
import os

from dotenv import load_dotenv
from flask import Flask, request, abort, send_from_directory
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


# Search and suggestions API


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


# Dashboards API


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


@app.route('/api/dashboard/info', methods=['GET'])
@cross_origin()
def get_dashboard_info():
    hash_code = request.args.get("hash_code")
    result = check_hash_code_and_call_resolver(hash_code, resolve_get_dashboard_info)
    return json.dumps(result)


def edit_dashboard(body):
    if body is None or "hashCode" not in body:
        abort(http.HTTPStatus.INTERNAL_SERVER_ERROR, "Please include hash code")
    result = resolve_edit_dashboard(body)
    if "errorMessage" in result:
        abort(http.HTTPStatus.INTERNAL_SERVER_ERROR, result["errorMessage"])
    return json.dumps(result)


@app.route('/api/dashboard/edit/global', methods=['POST'])
@cross_origin()
def edit_dashboard_global():
    body = request.json
    return edit_dashboard(body)


@app.route('/api/dashboard/edit/compare', methods=['POST'])
@cross_origin()
def edit_dashboard_compare():
    body = request.json
    if 'compareFilters' not in body:
        result = {"erorMessage": "compareFilters not in body"}
        return json.dumps(result)
    return edit_dashboard(body)


@app.route('/api/dashboard/edit/analysis', methods=['POST'])
@cross_origin()
def edit_dashboard_analysis():
    body = request.json
    if 'analysisFilters' not in body:
        result = {"erorMessage": "analysisFilters not in body"}
        return json.dumps(result)
    return edit_dashboard(body)


@app.route('/api/dashboard/duplicate', methods=['GET'])
@cross_origin()
def get_duplicate_dashboard():
    hash_code = request.args.get("hash_code")
    result = check_hash_code_and_call_resolver(hash_code, resolve_duplicate_dashboard)
    return json.dumps(result)


@app.route('/api/dashboard/status', methods=['GET'])
@cross_origin()
def get_dashboard_new_status():
    hash_code = request.args.get("hash_code")
    if hash_code == "" or hash_code is None:
        abort(http.HTTPStatus.INTERNAL_SERVER_ERROR, "Please include dashboard hashcode")
    status = request.args.get("status")
    if status == "" or status is None:
        abort(http.HTTPStatus.INTERNAL_SERVER_ERROR, "Please include dashboard status")
    result = resolve_set_dashboard_status(hash_code, status)
    if "errorMessage" in result:
        abort(http.HTTPStatus.INTERNAL_SERVER_ERROR, result["errorMessage"])
    return json.dumps(result)


@app.route('/api/dashboard/delete', methods=['GET'])
@cross_origin()
def delete_dashboard():
    hash_code = request.args.get("hash_code")
    result = check_hash_code_and_call_resolver(hash_code, resolve_delete_dashboard)
    return json.dumps(result)


@app.route('/api/properties/info', methods=['GET'])
@cross_origin()
def get_properties_info():
    hash_code = request.args.get("hash_code")
    if hash_code == "" or hash_code is None:
        abort(http.HTTPStatus.INTERNAL_SERVER_ERROR, "Please include dashboard hashcode")
    result = resolve_get_properties_info(hash_code)
    return json.dumps(result)


# Profile view API

@app.route('/api/entity/gini', methods=['GET', 'POST'])
@cross_origin()
def get_gini_with_filters():
    if request.method == 'GET':
        hash_code = request.args.get('hash_code')
        if hash_code == "" or hash_code is None:
            abort(http.HTTPStatus.INTERNAL_SERVER_ERROR, "Please include hash code")
        prop = request.args.get('property')
        result = {}
        try:
            result = resolve_get_entity_gini_by_hash(hash_code, prop)
        except Exception as e:
            abort(http.HTTPStatus.INTERNAL_SERVER_ERROR, str(e))
        if "errorMessage" in result:
            abort(http.HTTPStatus.INTERNAL_SERVER_ERROR, result["errorMessage"])
        return json.dumps(result)
    elif request.method == 'POST':
        body = request.json
        if 'entity' not in body or 'filters' not in body:
            abort(http.HTTPStatus.INTERNAL_SERVER_ERROR, "Invalid Body")
        entity = body['entity']
        filters = body['filters']
        result = resolve_gini_with_filters_unbounded(entity, filters, "")
        return json.dumps(result)


@app.route('/api/download/sparql', methods=['GET'])
@cross_origin()
def get_sparql_file():
    file_name = request.args.get("file_name")
    if file_name == "" or file_name is None:
        abort(http.HTTPStatus.INTERNAL_SERVER_ERROR, "Please include file_name")
    try:
        return send_from_directory(app.config["CLIENT_SPARQL"], filename=file_name, as_attachment=True)
    except FileNotFoundError:
        abort(404, "File not found")


@app.route('/api/download/csv', methods=['GET'])
@cross_origin()
def get_csv_file():
    file_name = request.args.get("file_name")
    if file_name == "" or file_name is None:
        abort(http.HTTPStatus.INTERNAL_SERVER_ERROR, "Please include file_name")
    try:
        return send_from_directory(app.config["CLIENT_CSV"], filename=file_name, as_attachment=True)
    except FileNotFoundError:
        abort(404, "File not found")


# Compare view API

@app.route('/api/entity/gini/compare', methods=['GET'])
@cross_origin()
def get_comparison_gini():
    hash_code = request.args.get("hash_code")
    item_number = request.args.get("item_number")
    comparison_check(hash_code, item_number)
    result = {}
    try:
        result = resolve_get_comparison_gini(hash_code, item_number)
    except Exception as e:
        abort(http.HTTPStatus.INTERNAL_SERVER_ERROR, str(e))

    if "errorMessage" in result:
        abort(http.HTTPStatus.INTERNAL_SERVER_ERROR, result["errorMessage"])
    return json.dumps(result)


@app.route('/api/property/compare', methods=['GET'])
@cross_origin()
def get_comparison_properties():
    hash_code = request.args.get("hash_code")
    item_number = request.args.get("item_number")
    comparison_check(hash_code, item_number)
    result = resolve_get_comparison_properties(hash_code, item_number)
    if "errorMessage" in result:
        abort(http.HTTPStatus.INTERNAL_SERVER_ERROR, result["errorMessage"])
    return json.dumps(result)


@app.route('/api/properties/info/compare', methods=['GET'])
@cross_origin()
def get_properties_info_compare():
    hash_code = request.args.get("hash_code")
    if hash_code == "" or hash_code is None:
        abort(http.HTTPStatus.INTERNAL_SERVER_ERROR, "Please include dashboard hashcode")
    result = resolve_get_properties_info_compare(hash_code)
    return json.dumps(result)


def comparison_check(hash_code, item_number):
    if hash_code == "" or hash_code is None:
        abort(http.HTTPStatus.INTERNAL_SERVER_ERROR, "Please include dashboard hash_code")
    if item_number == "" or item_number is None:
        abort(http.HTTPStatus.INTERNAL_SERVER_ERROR, "Please include item_number")
    if item_number != "1" and item_number != "2":
        abort(http.HTTPStatus.INTERNAL_SERVER_ERROR, "Invalid item_number")


# Analysis view API


@app.route('/api/entity/analysis/information', methods=['GET'])
@cross_origin()
def get_analysis_information():
    hash_code = request.args.get("hash_code")
    if hash_code == "" or hash_code is None:
        abort(http.HTTPStatus.INTERNAL_SERVER_ERROR, "Please include dashboard hash_code")
    result = resolve_get_analysis_information(hash_code)
    if "errorMessage" in result:
        abort(http.HTTPStatus.INTERNAL_SERVER_ERROR, result["errorMessage"])
    return json.dumps(result)


@app.route('/api/entity/gini/analysis', methods=['GET'])
@cross_origin()
def get_gini_analysis():
    hash_code = request.args.get("hash_code")
    if hash_code == "" or hash_code is None:
        abort(http.HTTPStatus.INTERNAL_SERVER_ERROR, "Please include dashboard hash_code")
    result = {}
    try:
        result = resolve_get_gini_analysis(hash_code)
    except Exception as e:
        abort(http.HTTPStatus.INTERNAL_SERVER_ERROR, str(e))
    if "errorMessage" in result:
        abort(http.HTTPStatus.INTERNAL_SERVER_ERROR, result["errorMessage"])
    return json.dumps(result)


@app.route('/api/property/analysis', methods=['GET'])
@cross_origin()
def get_property_analysis():
    hash_code = request.args.get("hash_code")
    if hash_code == "" or hash_code is None:
        abort(http.HTTPStatus.INTERNAL_SERVER_ERROR, "Please include dashboard hash_code")
    property_id = request.args.get("property_id")
    if property_id == "" or property_id is None:
        abort(http.HTTPStatus.INTERNAL_SERVER_ERROR, "Please include property id")
    entity_id = request.args.get("entity_id")
    if entity_id == "" or entity_id is None:
        abort(http.HTTPStatus.INTERNAL_SERVER_ERROR, "Please include entity id")
    result = resolve_get_property_analysis(hash_code, property_id, entity_id)
    if "errorMessage" in result:
        abort(http.HTTPStatus.INTERNAL_SERVER_ERROR, result["errorMessage"])
    return json.dumps(result)


@app.route('/api/analysis/custom', methods=['POST'])
@cross_origin()
def set_analysis_custom():
    body = request.json
    if "hash_code" not in body:
        abort(http.HTTPStatus.INTERNAL_SERVER_ERROR, "Please include dashboard hash_code in body")
    hash_code = body["hash_code"]
    min_filter = body.get("min_filter", "")
    max_filter = body.get("max_filter", "")
    shown_combinations = body.get("shown_combinations", "")
    result = resolve_set_analysis_custom(hash_code, min_filter, max_filter, shown_combinations)
    if "errorMessage" in result:
        abort(http.HTTPStatus.INTERNAL_SERVER_ERROR, result["errorMessage"])
    return json.dumps(result)


# Utils API

@app.route('/api/browse', methods=['GET'])
@cross_origin()
def get_all_profiles():
    result = resolve_get_all_profiles()
    return json.dumps(result)


@app.route('/api/entities/count', methods=['GET'])
@cross_origin()
def get_entities_count():
    hash_code = request.args.get("hash_code")
    result = check_hash_code_and_call_resolver(hash_code, resolve_get_entities_count)
    return json.dumps(result)


@app.route('/api/entity/info', methods=['GET'])
@cross_origin()
def get_entity_info():
    entity_id = request.args.get("entity_id")
    if entity_id == "" or entity_id is None:
        abort(abort(http.HTTPStatus.INTERNAL_SERVER_ERROR, "Please include entity_id"))
    result = resolve_get_entity_info(entity_id)
    return json.dumps(result)


def check_hash_code_and_call_resolver(hash_code, resolver):
    if hash_code == "" or hash_code is None:
        abort(http.HTTPStatus.INTERNAL_SERVER_ERROR, "Please include dashboard hashcode")
    result = resolver(hash_code)
    if "errorMessage" in result:
        abort(http.HTTPStatus.INTERNAL_SERVER_ERROR, result["errorMessage"])
    return result


@app.route('/api/wikidata/gini/analysis', methods=['POST'])
@cross_origin()
def get_wikidata_gini_analysis():
    data = request.json
    result = resolve_get_wikidata_gini_analysis(data)
    return json.dumps(result)


if __name__ == '__main__':
    app.run(debug=True)
