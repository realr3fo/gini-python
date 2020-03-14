#!venv/bin/python3
import http
import json

from flask import Flask, request, abort
from flask_cors import CORS, cross_origin
from resolver import resolve_unbounded, resolve_bounded

app = Flask(__name__)
cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'


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


if __name__ == '__main__':
    app.run(debug=True)
