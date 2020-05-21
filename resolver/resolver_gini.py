import asyncio

from utils.gini import construct_results_gini
from utils.wikidata import async_get_results


def get_each_amount_bounded(chunked_q_arr):
    result = []
    for elem in chunked_q_arr:
        result.append(len(elem))
    return result


async def get_gini_from_wikidata(entity, filter_query, has_property_query, offset_count):
    from resolver.resolver import ENDPOINT_URL
    limit = 2000
    offset = offset_count * 2000
    query = """
            SELECT ?item ?itemLabel ?cnt ?p1 {
                {SELECT ?item (COUNT(DISTINCT(?prop)) AS ?cnt) ?p1 {

                {SELECT DISTINCT ?item WHERE {
                   ?item wdt:P31 wd:%s .
                   %s
                } LIMIT %d OFFSET %d}
                OPTIONAL { ?item ?p ?o . FILTER(CONTAINS(STR(?p),"http://www.wikidata.org/prop/direct/")) 
                ?prop wikibase:directClaim ?p . FILTER NOT EXISTS {?prop wikibase:propertyType wikibase:ExternalId .} }
                %s

                } GROUP BY ?item ?p1}

                SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }

                } ORDER BY DESC(?cnt)
            """ % (entity, filter_query, limit, offset, has_property_query)
    query_results = await async_get_results(ENDPOINT_URL, query)
    return query_results["results"]["bindings"]


def create_query(entity, filter_query, has_property_query, limit=1000):
    query = """SELECT ?item ?itemLabel ?cnt ?p1 {
    {SELECT ?item (COUNT(DISTINCT(?prop)) AS ?cnt) ?p1 {
    {SELECT DISTINCT ?item WHERE {
       ?item wdt:P31 wd:%s .
       %s
    } LIMIT %d}
    OPTIONAL { ?item ?p ?o . FILTER(CONTAINS(STR(?p),"http://www.wikidata.org/prop/direct/")) 
    ?prop wikibase:directClaim ?p . FILTER NOT EXISTS {?prop wikibase:propertyType wikibase:ExternalId .} }
    %s
    } GROUP BY ?item ?p1}
    SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
} ORDER BY DESC(?cnt)
                """ % (entity, filter_query, limit, has_property_query)
    return query


def resolve_gini_with_filters_unbounded(entity, filters, has_property):
    filter_query = ""
    for elem in filters:
        for elem_filter in elem.keys():
            filter_query += "?item wdt:%s wd:%s . " % (elem_filter, elem[elem_filter])
    has_property_query = ""
    if has_property != "" and has_property is not None:
        has_property_query += " OPTIONAL { ?item wdt:%s _:v1 . BIND (1 AS ?p1) } OPTIONAL { BIND (0 AS ?p1) } " % has_property
    query = create_query(entity, filter_query, has_property_query)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    tasks = []
    # create instance of Semaphore
    for i in range(5):
        task = loop.create_task(get_gini_from_wikidata(entity, filter_query, has_property_query, i))
        tasks.append(task)
    query_results_arr = loop.run_until_complete(asyncio.gather(*tasks))
    loop.close()
    query_results_bindings = [item for sublist in query_results_arr for item in sublist]
    item_arr = query_results_bindings
    if len(item_arr) == 0:
        result = {
            "gini": 0, "data": [], "entities": []}
        return result
    q_arr = []
    for elem in item_arr:
        item_link = elem['item']['value']
        item_id = item_link.split("/")[-1]
        property_count = int(elem['cnt']['value'])
        item_label = elem['itemLabel']['value']
        item_has_property = None
        if 'p1' in elem:
            item_has_property = elem['p1']['value']
            if item_has_property == '1':
                item_has_property = True
            else:
                item_has_property = False
        entity_obj = (item_id, property_count, item_label, item_link, item_has_property)
        q_arr.append(entity_obj)
    result = construct_results_gini(q_arr, "profile", query)
    return result
