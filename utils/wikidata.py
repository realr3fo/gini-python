import sys
from SPARQLWrapper import SPARQLWrapper, JSON
from aiosparql.client import SPARQLClient


def get_results(endpoint_url, query):
    user_agent = "WDQS-example Python/%s.%s" % (sys.version_info[0], sys.version_info[1])
    sparql = SPARQLWrapper(endpoint_url, agent=user_agent)
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    return sparql.query().convert()


async def async_get_results(endpoint_url, query):
    sparql = SPARQLClient(endpoint_url)
    result = await sparql.query(query)
    await sparql.close()
    return result
