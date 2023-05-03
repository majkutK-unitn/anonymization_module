from interfaces.mondrian_api import MondrianAPI

from elasticsearch import Elasticsearch
from os import getenv


class EsConnector(MondrianAPI):

    def __init__(self):
        API_KEY_ID = getenv('ES_API_KEY_ID')
        API_KEY_SECRET = getenv('ES_API_KEY_SECRET')
        ROOT_CA_PATH = getenv('ROOT_CA_PATH')

        self.es = Elasticsearch(
                hosts="https://neteye2.test:9200",
                api_key=(API_KEY_ID, API_KEY_SECRET), 
                ca_certs=ROOT_CA_PATH
            )    


    def search(self, query):                
        res = self.es.search(index="adults", query=query)                
        for hit in res['hits']['hits']:
            print("%(age)s %(native_country)s: %(education)s" % hit["_source"])


    def get_median(self):        
        query = {
            "aggs": {
                "age_median": { "percentiles": { "field": "age", "percents": [ 50 ] }},
                "age_value_after_median": { "percentiles": { "field": "age", "percents": [ 52 ] }},
                "age_min": { "min": { "field": "age" } },
                "age_max": { "max": { "field": "age" } }
            }
        }

        return self.search(query)