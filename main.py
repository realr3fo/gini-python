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


@app.route('/api/gini', methods=['GET'])
@cross_origin()
def get_gini():
    entity = request.args.get('entity')
    if entity is None or entity == "":
        abort(http.HTTPStatus.INTERNAL_SERVER_ERROR, "Please input the WikiData entity ID in query parameters")

    properties = request.args.get('properties')
    if properties is None or properties == "[]" or len(properties) == 0:
        result = resolve_unbounded(entity)
    else:
        result = resolve_bounded(entity, properties)

    return json.dumps(result)


@app.route('/api/property/gap', methods=['POST'])
@cross_origin()
def get_property_gap():
    entities = request.json
    if "entities" not in entities:
        abort(http.HTTPStatus.INTERNAL_SERVER_ERROR, "Please include entities objects")
    entities = entities["entities"]
    if len(entities) == 0:
        abort(http.HTTPStatus.INTERNAL_SERVER_ERROR, "Please do not send empty array of entities objects")
    result = resolve_property_gap(entities)
    return result


@app.route('/api/property/analysis', methods=['POST'])
@cross_origin()
def get_property_analysis():
    analysis_type = request.args.get('type')
    entities = request.json
    if "entities" not in entities:
        abort(http.HTTPStatus.INTERNAL_SERVER_ERROR, "Please include entities objects")
    entities = entities["entities"]
    if len(entities) == 0:
        abort(http.HTTPStatus.INTERNAL_SERVER_ERROR, "Please do not send empty array of entities objects")
    if analysis_type == "intersection top intersection bot":
        result = resolve_property_gap_intersection_top_intersection_bot(entities)
    elif analysis_type == "union top intersection bot":
        result = resolve_property_gap_intersection_top_union_bot(entities)
    else:
        result = resolve_property_gap_union_top_union_bot(entities)
    return result


@app.route('/api/gini/analysis', methods=['GET'])
@cross_origin()
def gini_entities_analysis():
    limit = request.args.get('limit')
    if limit == "":
        abort(http.HTTPStatus.INTERNAL_SERVER_ERROR, "Please include limit")
    limit = int(limit)
    result = resolve_gini_analysis(limit)
    return result


@app.route('/api/entity/gini', methods=['GET', 'POST'])
@cross_origin()
def get_gini_with_filters():
    if request.method == 'GET':
        hash_code = request.args.get('hash')
        if hash_code == "":
            abort(http.HTTPStatus.INTERNAL_SERVER_ERROR, "Please include hash code")
        result = resolve_gini_with_filters(hash_code)
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
    result = resolve_get_wikidata_entities()
    return json.dumps(result)


@app.route('/api/filter/suggestions', methods=['GET'])
@cross_origin()
def get_filter_suggestions():
    entity_id = request.args.get('entity_id')
    if entity_id == "":
        abort(http.HTTPStatus.INTERNAL_SERVER_ERROR, "Please include entity id")
    result = resolve_get_filter_suggestions(entity_id)
    return json.dumps(result)


if __name__ == '__main__':
    app.run(debug=True)
