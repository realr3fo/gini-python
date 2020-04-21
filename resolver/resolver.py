import time
import uuid

from main import db
from models.models import Dashboards
from resolver.resolve_information import resolve_get_entity_information_result, resolve_get_properties_info_result
from resolver.resolve_property_gap import resolve_get_property_gap_bounded_api_sandbox, \
    resolve_get_property_gap_unbounded_api_sandbox
from resolver.resolve_suggestions import resolve_get_wikidata_properties_result, \
    resolve_get_filter_suggestions_result, resolve_get_wikidata_entities_result
from resolver.resolver_gini import resolve_gini_with_filters_unbounded, resolve_gini_with_filters_bounded

ENDPOINT_URL = "https://query.wikidata.org/sparql"
LIMITS = {"unbounded": 10000, "bounded": 10000, "property_gap": 1000}


def resolve_test_json():
    result = {}
    return result


def resolve_get_wikidata_entities(search):
    return resolve_get_wikidata_entities_result(search)


def resolve_get_filter_suggestions(entity_id, filled_properties):
    return resolve_get_filter_suggestions_result(entity_id, filled_properties)


def resolve_get_wikidata_properties(search):
    return resolve_get_wikidata_properties_result(search)


def resolve_create_dashboard(entity_id, filters):
    uuid_string = str(uuid.uuid4())
    hash_code = uuid_string.split("-")[-1]
    if filters == "" or filters is None:
        filters = "[]"
    data = {'name': "", 'author': "", 'entity': entity_id, 'hash_code': hash_code, 'filters': str(filters),
            'properties': "[]"}
    save_dashboard_to_db(data)
    result = {"hashCode": hash_code}
    return result


def resolve_get_entity_information(hash_code):
    single_dashboard = Dashboards.query.filter_by(hash_code=hash_code).first()
    return resolve_get_entity_information_result(single_dashboard)


def resolve_get_properties_info(hash_code):
    single_dashboard = Dashboards.query.filter_by(hash_code=hash_code).first()
    return resolve_get_properties_info_result(single_dashboard)


def resolve_get_property_gap_api_sandbox(entities):
    if len(entities) == 0:
        return {"errorMessage": "list of entities is impty"}
    sample_entity_obj = entities[0]
    if "entityProperties" in sample_entity_obj:
        return resolve_get_property_gap_bounded_api_sandbox(entities)
    else:
        return resolve_get_property_gap_unbounded_api_sandbox(entities)


def resolve_get_entity_gini_by_hash(hash_code):
    single_dashboard = Dashboards.query.filter_by(hash_code=hash_code).first()
    if single_dashboard is None:
        return {"errorMessage": "hash not found"}

    entity_id = single_dashboard.entity
    filters = eval(single_dashboard.filters)
    properties = eval(single_dashboard.properties)

    if len(properties) == 0:
        result = resolve_gini_with_filters_unbounded(entity_id, filters)
    else:
        result = resolve_gini_with_filters_bounded(entity_id, filters, properties)

    return result


def save_dashboard_to_db(data):
    name = data['name']
    author = data['author']
    entity = data['entity']
    hash_code = data['hash_code']
    filters = data['filters']
    properties = data['properties']
    timestamp = str(time.time())
    try:
        dashboard = Dashboards(
            name=name,
            author=author,
            entity=entity,
            hash_code=hash_code,
            filters=filters,
            properties=properties,
            timestamp=timestamp
        )
        db.session.add(dashboard)
        db.session.commit()
    except Exception as e:
        return str(e)
