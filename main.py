#!venv/bin/python3
import http
import json
import os
import time
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

from resolver import resolve_unbounded, resolve_bounded, resolve_property_gap, \
    resolve_property_gap_intersection_top_intersection_bot, resolve_property_gap_intersection_top_union_bot


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
    entities = request.json
    if "entities" not in entities:
        abort(http.HTTPStatus.INTERNAL_SERVER_ERROR, "Please include entities objects")
    entities = entities["entities"]
    if len(entities) == 0:
        abort(http.HTTPStatus.INTERNAL_SERVER_ERROR, "Please do not send empty array of entities objects")
    result = resolve_property_gap_intersection_top_union_bot(entities)
    return result


if __name__ == '__main__':
    app.run(debug=True)
