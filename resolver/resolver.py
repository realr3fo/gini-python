import json
import time
import uuid

from main import db
from models.models import Dashboards
from resolver.resolve_information import resolve_get_entity_information_result, resolve_get_properties_info_result
from resolver.resolve_property_gap import resolve_get_property_gap_bounded_api_sandbox, \
    resolve_get_property_gap_unbounded_api_sandbox
from resolver.resolve_suggestions import resolve_get_wikidata_properties_result, \
    resolve_get_filter_suggestions_result, resolve_get_wikidata_entities_result
from resolver.resolver_comparison import resolve_get_comparison_gini_result
from resolver.resolver_gini import resolve_gini_with_filters_unbounded, resolve_gini_with_filters_bounded

ENDPOINT_URL = "https://query.wikidata.org/sparql"
LIMITS = {"unbounded": 10000, "bounded": 10000, "property_gap": 1000}


def resolve_get_wikidata_entities(search):
    return resolve_get_wikidata_entities_result(search)


def resolve_get_filter_suggestions(entity_id, filled_properties):
    return resolve_get_filter_suggestions_result(entity_id, filled_properties)


def resolve_get_wikidata_properties(search):
    return resolve_get_wikidata_properties_result(search)


def resolve_create_dashboard(entity_id, filters):
    uuid_string = str(uuid.uuid4())
    hash_code = uuid_string.split("-")[-1]
    data = {'name': "", 'author': "", 'entity': entity_id, 'hash_code': hash_code}
    if filters != "" or filters is not None:
        data['filters'] = filters
    save_status = save_dashboard_to_db(data)

    if save_status != "success":
        return {"errorMessage": save_status}

    result = {"hashCode": hash_code}

    return result


def resolve_edit_dashboard(data):
    hash_code = data["hashCode"]
    single_dashboard = Dashboards.query.filter_by(hash_code=hash_code).first()
    if single_dashboard is None:
        return {"errorMessage": "data with the given hash code was not found"}
    entity_name = single_dashboard.name
    entity_author = single_dashboard.author
    entity_id = single_dashboard.entity
    entity_filters = single_dashboard.filters
    entity_properties = single_dashboard.properties
    entity_additional_filters = single_dashboard.additional_filters
    entity_compare_filters = single_dashboard.compare_filters
    entity_analysis_filters = single_dashboard.analysis_filters

    if "entityID" in data:
        entity_id = str(data["entityID"])
    if "filters" in data:
        entity_filters = str(data["filters"])
    if "properties" in data:
        entity_properties = str(data["properties"])
    if "additionalFilters" in data:
        entity_additional_filters = str(data["additionalFilters"])
    if "compareFilters" in data:
        entity_compare_filters = str(data["compareFilters"])
    if "analysisFilters" in data:
        entity_analysis_filters = str(data["analysisFilters"])
    single_dashboard.entity = entity_id
    single_dashboard.filters = entity_filters
    single_dashboard.properties = entity_properties
    single_dashboard.additional_filters = entity_additional_filters
    single_dashboard.compare_filters = entity_compare_filters
    single_dashboard.analysis_filters = entity_analysis_filters

    if "entityID" in data or "filters" in data or "properties" in data:
        single_dashboard.instances = {}

    db.session.commit()
    updated_dashboard = single_dashboard.serialize()
    result = {"result": updated_dashboard}
    return result


def resolve_get_entity_information(hash_code):
    single_dashboard = Dashboards.query.filter_by(hash_code=hash_code).first()
    return resolve_get_entity_information_result(single_dashboard)


def resolve_get_properties_info(hash_code):
    single_dashboard = Dashboards.query.filter_by(hash_code=hash_code).first()
    return resolve_get_properties_info_result(single_dashboard)


def resolve_get_property_gap_api_sandbox(hash_code):
    single_dashboard = Dashboards.query.filter_by(hash_code=hash_code).first()
    entities = single_dashboard.instances["entities"]
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
        return {"errorMessage": "data with the given hash code was not found"}

    entity_id = single_dashboard.entity
    filters = eval(single_dashboard.filters)
    properties = eval(single_dashboard.properties)

    if len(properties) == 0:
        result = resolve_gini_with_filters_unbounded(entity_id, filters)
        entities = {"entities": result["entities"]}
        json_entities = json.loads(json.dumps(entities))
        single_dashboard.instances = json_entities
        db.session.commit()
    else:
        result = resolve_gini_with_filters_bounded(entity_id, filters, properties)
        entities = {"entities": result["entities"]}
        json_entities = json.loads(json.dumps(entities))
        single_dashboard.instances = json_entities
        db.session.commit()

    return result


def resolve_get_all_profiles():
    dashboards = Dashboards.query.all()
    profiles = []
    for dashboard in dashboards:
        entity_id = dashboard.entity
        profile_hash_code = dashboard.hash_code
        profile_filters = dashboard.filters
        profile_properties = dashboard.properties
        profile_obj = {"entityID": entity_id, "profileHashCode": profile_hash_code, "profileFilters": profile_filters,
                       "profileProperties": profile_properties}
        profiles.append(profile_obj)
    result = {"profiles": profiles}
    return result


def resolve_get_comparison_gini(hash_code, item_number):
    single_dashboard = Dashboards.query.filter_by(hash_code=hash_code).first()
    if single_dashboard is None:
        return {"errorMessage": "data with the given hash code was not found"}
    result = resolve_get_comparison_gini_result(single_dashboard, item_number)
    return result


def save_dashboard_to_db(data):
    name = data['name']
    author = data['author']
    entity = data['entity']
    hash_code = data['hash_code']
    timestamp = str(time.time())
    try:
        dashboard = Dashboards(
            name=name,
            author=author,
            entity=entity,
            hash_code=hash_code,
            timestamp=timestamp,
        )
        if 'filters' in data:
            dashboard.filters = str(data['filters'])
        db.session.add(dashboard)
        db.session.commit()
        return "success"
    except Exception as e:
        return str(e)
