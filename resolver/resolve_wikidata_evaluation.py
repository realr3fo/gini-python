import asyncio

from utils.wikidata import get_results, async_get_results


def get_wiikidata_classes(limit):
    from resolver.resolver import ENDPOINT_URL
    query = """SELECT DISTINCT ?o
WHERE {{
   ?s wdt:P31 ?o . 
}} limit {limit}
    """.format(limit=limit)
    query_results = get_results(ENDPOINT_URL, query)
    bindings = query_results["results"]["bindings"]
    classes = []
    for elem in bindings:
        class_link = elem["o"]["value"]
        class_id = class_link.split("/")[-1]
        classes.append(class_id)
    return classes


async def get_instances_from_class(wikidata_class, offset_count):
    from resolver.resolver import ENDPOINT_URL
    limit = 1000
    offset = offset_count * 1000
    query = """SELECT ?item ?itemLabel ?cnt{
  {SELECT ?item (COUNT(DISTINCT(?prop)) AS ?cnt) {
    {SELECT DISTINCT ?item WHERE {
      ?item wdt:P31 wd:%s.
    } LIMIT %d OFFSET %d}
    OPTIONAL { ?item ?p ?o . FILTER(CONTAINS(STR(?p),"http://www.wikidata.org/prop/direct/")) 
              ?prop wikibase:directClaim ?p . FILTER NOT EXISTS {?prop wikibase:propertyType wikibase:ExternalId .} }
  } GROUP BY ?item}
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
} ORDER BY DESC(?cnt)
                """ % (wikidata_class, limit, offset)
    query_results = await async_get_results(ENDPOINT_URL, query)
    return query_results["results"]["bindings"]


def async_call_get_instances(wikidata_class):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    tasks = []
    # create instance of Semaphore
    for i in range(5):
        task = loop.create_task(get_instances_from_class(wikidata_class, i))
        tasks.append(task)
    query_results_arr = loop.run_until_complete(asyncio.gather(*tasks))
    loop.close()
    query_results_bindings = [item for sublist in query_results_arr for item in sublist]
    item_arr = query_results_bindings
    return item_arr


def get_class_label(wikidata_class):
    query = """select ?item ?itemLabel {
  bind(wd:%s as ?item)  
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
}
    """ % wikidata_class
    from resolver.resolver import ENDPOINT_URL
    query_results = get_results(ENDPOINT_URL, query)
    bindings = query_results["results"]["bindings"]
    return bindings[0]["itemLabel"]["value"]


def calculate_gini(q_arr):
    from statistics import mean
    property_counts = sorted(q_arr, reverse=True)
    myu = mean(property_counts)
    n = len(property_counts)
    sum_y = sum((i + 1) * property_counts[i] for i in range(n))
    result = 1 + (1 / n) - ((2 / (n * n * myu)) * sum_y)
    return round(result, 3)


def calculate_gini_from_instances(instances):
    if len(instances) == 0:
        return 0
    property_counts = []
    for elem in instances:
        property_count = int(elem['cnt']['value'])
        property_counts.append(property_count)
    property_counts = sorted(property_counts)
    result = calculate_gini(property_counts)
    return result


def resolve_get_wikidata_gini_analysis_result(limit):
    wikidata_classes = get_wiikidata_classes(limit)
    classes_analysis = []
    for wikidata_class in wikidata_classes:
        class_id = wikidata_class
        class_label = get_class_label(wikidata_class)

        instances = async_call_get_instances(wikidata_class)

        amount = len(instances)
        gini = calculate_gini_from_instances(instances)
        result_obj = {"id": class_id, "label": class_label, "amount": amount, "gini": gini}
        classes_analysis.append(result_obj)
        print(result_obj)

    result = {"result": classes_analysis}
    return result
