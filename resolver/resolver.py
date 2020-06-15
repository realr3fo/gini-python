import json
import time
import uuid

from main import db
from models.models import Dashboards, Analysis
from resolver.resolve_analysis import resolve_get_analysis_information_result, resolve_get_gini_analysis_result, \
    resolve_get_property_analysis_result
from resolver.resolve_card import resolve_get_entities_count_result, resolve_get_entities_same_as_profile
from resolver.resolve_information import resolve_get_entity_information_result, resolve_get_properties_info_result, \
    resolve_get_dashboard_info_result, resolve_get_properties_info_compare_result, \
    resolve_get_compare_filters_info_result, resolve_get_analysis_properties_info_result, resolve_get_entity_info_result
from resolver.resolve_suggestions import resolve_get_wikidata_properties_result, \
    resolve_get_filter_suggestions_result, resolve_get_wikidata_entities_result
from resolver.resolve_wikidata_evaluation import resolve_get_wikidata_gini_analysis_result
from resolver.resolver_comparison import resolve_get_comparison_gini_result, resolve_get_comparison_properties_result
from resolver.resolver_gini import resolve_gini_with_filters_unbounded

ENDPOINT_URL = "https://query.wikidata.org/sparql"
LIMITS = {"unbounded": 10000, "bounded": 10000, "property_gap": 1000}


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
        if 'compareFilters' in data:
            dashboard.compare_filters = str(data['compareFilters'])
        if 'analysisFilters' in data:
            dashboard.analysis_filters = str(data['analysisFilters'])

        db.session.add(dashboard)
        db.session.flush()
        analysis = Analysis(
            dashboard_id=dashboard.id
        )
        db.session.add(analysis)
        db.session.commit()
        return "success"
    except Exception as e:
        return str(e)


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

    if "name" in data:
        entity_name = data["name"]
    if "author" in data:
        entity_author = data["author"]
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
    single_dashboard.name = entity_name
    single_dashboard.author = entity_author
    single_dashboard.entity = entity_id
    single_dashboard.filters = entity_filters
    single_dashboard.properties = entity_properties
    single_dashboard.additional_filters = entity_additional_filters
    single_dashboard.compare_filters = entity_compare_filters
    single_dashboard.analysis_filters = entity_analysis_filters

    entity_info = resolve_get_entity_information_result(single_dashboard)
    single_dashboard.entity_info = entity_info["entity"]
    single_dashboard.filters_info = entity_info["filters"]
    single_dashboard.properties_info = entity_info["properties"]

    analysis_info = resolve_get_analysis_properties_info_result(single_dashboard)
    single_dashboard.analysis_info = analysis_info

    # if "entityID" in data or "filters" in data or "properties" in data:
    #     single_dashboard.instances = {}

    db.session.commit()
    result = {"result": "success"}
    return result


def resolve_get_properties_info(hash_code):
    single_dashboard = Dashboards.query.filter_by(hash_code=hash_code).first()
    return resolve_get_properties_info_result(single_dashboard)


# def resolve_get_property_gap_api_sandbox(hash_code):
#     single_dashboard = Dashboards.query.filter_by(hash_code=hash_code).first()
#     instances = single_dashboard.instances
#     if "entities" not in instances:
#         return {"errorMessage", "entities not found"}
#     entities = instances["entities"]
#     if len(entities) == 0:
#         return {"errorMessage": "list of entities is impty"}
#     sample_entity_obj = entities[0]
#     if "entityProperties" in sample_entity_obj:
#         return resolve_get_property_gap_bounded_api_sandbox(entities)
#     else:
#         return resolve_get_property_gap_unbounded_api_sandbox(entities)


def resolve_get_entity_gini_by_hash(hash_code, prop):
    single_dashboard = Dashboards.query.filter_by(hash_code=hash_code).first()
    if single_dashboard is None:
        return {"errorMessage": "data with the given hash code was not found"}

    entity_id = single_dashboard.entity
    filters = eval(single_dashboard.filters)
    has_property = prop

    result = resolve_gini_with_filters_unbounded(entity_id, filters, has_property)

    return result


def resolve_get_all_profiles():
    dashboards = Dashboards.query.all()
    profiles = []
    for dashboard in dashboards:
        dashboard_data = dashboard.serialize()
        del dashboard_data['id']
        for elem in dashboard_data.keys():
            try:
                dashboard_data[elem] = eval(dashboard_data[elem])
            except Exception:
                pass
        profiles.append(dashboard_data)

    result = {"profiles": profiles}
    return result


def resolve_get_comparison_gini(hash_code, item_number):
    single_dashboard = Dashboards.query.filter_by(hash_code=hash_code).first()
    if single_dashboard is None:
        return {"errorMessage": "data with the given hash code was not found"}
    result = resolve_get_comparison_gini_result(single_dashboard, item_number)
    return result


def resolve_get_comparison_properties(hash_code, item_number):
    single_dashboard = Dashboards.query.filter_by(hash_code=hash_code).first()
    if single_dashboard is None:
        return {"errorMessage": "data with the given hash code was not found"}
    result = resolve_get_comparison_properties_result(single_dashboard, item_number)
    return result


def resolve_get_analysis_information(hash_code):
    single_dashboard = Dashboards.query.filter_by(hash_code=hash_code).first()
    if single_dashboard is None:
        return {"errorMessage": "data with the given hash code was not found"}
    result = resolve_get_analysis_information_result(single_dashboard)
    return result


def resolve_get_gini_analysis(hash_code):
    single_dashboard = Dashboards.query.filter_by(hash_code=hash_code).first()
    if single_dashboard is None:
        return {"errorMessage": "data with the given hash code was not found"}
    analysis_dashboard = Analysis.query.filter_by(dashboard_id=single_dashboard.id).first()
    shown_combinations = analysis_dashboard.shown_combinations
    filter_limit = analysis_dashboard.filter_limit
    result = resolve_get_gini_analysis_result(single_dashboard, shown_combinations, filter_limit)
    analysis_dashboard.shown_combinations = result.get("combinations", {})
    analysis_dashboard.filter_limit = [2, result.get("max_number", 10000)]
    db.session.commit()
    return result


def resolve_get_property_analysis(hash_code, property_id, entity_id):
    single_dashboard = Dashboards.query.filter_by(hash_code=hash_code).first()
    if single_dashboard is None:
        return {"errorMessage": "data with the given hash code was not found"}
    result = resolve_get_property_analysis_result(single_dashboard, property_id, entity_id)
    return result


def resolve_get_entities_count(hash_code):
    single_dashboard = Dashboards.query.filter_by(hash_code=hash_code).first()
    if single_dashboard is None:
        return {"errorMessage": "data with the given hash code was not found"}
    result = resolve_get_entities_same_as_profile(single_dashboard)
    return result


def resolve_get_dashboard_info(hash_code):
    single_dashboard = Dashboards.query.filter_by(hash_code=hash_code).first()

    if single_dashboard is None:
        return {"errorMessage": "data with the given hash code was not found"}
    entity_info = resolve_get_entity_information_result(single_dashboard)
    single_dashboard.entity_info = entity_info["entity"]
    single_dashboard.filters_info = entity_info["filters"]
    single_dashboard.properties_info = entity_info["properties"]
    result = resolve_get_dashboard_info_result(single_dashboard)
    compare_info = resolve_get_compare_filters_info_result(single_dashboard)
    result["compareInfo"] = compare_info
    analysis_info = resolve_get_analysis_properties_info_result(single_dashboard)
    result["analysisInfo"] = analysis_info
    single_dashboard.analysis_info = analysis_info
    analysis_dashboard = Analysis.query.filter_by(dashboard_id=single_dashboard.id).first()
    result["filterLimit"] = analysis_dashboard.filter_limit
    result["shownCombinations"] = analysis_dashboard.shown_combinations

    db.session.commit()
    return result


def resolve_get_properties_info_compare(hash_code):
    single_dashboard = Dashboards.query.filter_by(hash_code=hash_code).first()
    if single_dashboard is None:
        return {"errorMessage": "data with the given hash code was not found"}
    return resolve_get_properties_info_compare_result(single_dashboard)


def resolve_get_entity_info(entity_id):
    result = resolve_get_entity_info_result(entity_id)
    return result


def resolve_duplicate_dashboard(hash_code):
    single_dashboard = Dashboards.query.filter_by(hash_code=hash_code).first()
    if single_dashboard is None:
        return {"errorMessage": "data with the given hash code was not found"}
    row_data = single_dashboard.serialize()
    timestamp = str(time.time())
    uuid_string = str(uuid.uuid4())
    duplicate_hash_code = uuid_string.split("-")[-1]
    data = {
        "name": row_data["name"],
        "author": row_data["author"],
        "entity": row_data["entity"],
        "hash_code": duplicate_hash_code,
        "timestamp": timestamp,
        "filters": row_data["filters"],
        "compareFilters": row_data["compareFilters"],
        "analysisFilters": row_data["analysisFilters"],
    }
    save_dashboard_to_db(data)

    result = {"hash_code": duplicate_hash_code}
    return result


def resolve_set_dashboard_status(hash_code, status):
    single_dashboard = Dashboards.query.filter_by(hash_code=hash_code).first()
    if single_dashboard is None:
        return {"errorMessage": "data with the given hash code was not found"}
    if status == "public":
        single_dashboard.public = True
    else:
        single_dashboard.public = False
    db.session.commit()
    result = {"result": "success"}
    return result


def resolve_set_analysis_custom(hash_code, min_filter, max_filter, shown_combinations):
    single_dashboard = Dashboards.query.filter_by(hash_code=hash_code).first()
    if single_dashboard is None:
        return {"errorMessage": "data with the given hash code was not found"}
    analysis_dashboard = Analysis.query.filter_by(dashboard_id=single_dashboard.id).first()
    analysis_combinations = analysis_dashboard.shown_combinations
    if shown_combinations != "":
        for comb in analysis_combinations:
            for elem in shown_combinations:
                if comb["item_1"] == elem["item_1"] and comb["item_2"] == elem["item_2"]:
                    comb["shown"] = elem["shown"]
                if comb["item_1"] == elem["item_1"] and "item_2" not in comb:
                    comb["shown"] = elem["shown"]
        from sqlalchemy.orm.attributes import flag_modified
        flag_modified(analysis_dashboard, "shown_combinations")
        analysis_dashboard.shown_combinations = analysis_combinations
    if min_filter != "" and max_filter != "":
        filter_limit = [int(min_filter), int(max_filter)]
        analysis_dashboard.filter_limit = filter_limit

    db.session.commit()
    result = {"result": "success"}
    return result


def resolve_delete_dashboard(hash_code):
    single_dashboard = Dashboards.query.filter_by(hash_code=hash_code).first()
    if single_dashboard is None:
        return {"errorMessage": "data with the given hash code was not found"}
    analysis_dashboard = Analysis.query.filter_by(dashboard_id=single_dashboard.id).first()
    db.session.delete(analysis_dashboard)
    db.session.delete(single_dashboard)
    db.session.commit()
    result = {"result": "success"}
    return result


def resolve_get_wikidata_gini_analysis(data):
    limit = data["limit"]
    result = resolve_get_wikidata_gini_analysis_result(limit)
    return result
