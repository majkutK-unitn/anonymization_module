from interfaces.mondrian_api import MondrianAPI

from elasticsearch import Elasticsearch
from os import getenv


class EsConnector(MondrianAPI):

    def __init__(self):
        self.API_KEY_ID = getenv('ES_API_KEY_ID')
        self.API_KEY_SECRET = getenv('ES_API_KEY_SECRET')
        self.ROOT_CA_PATH = getenv('ROOT_CA_PATH')

    def run_query(self, query):
        print(self.ROOT_CA_PATH)

        es = Elasticsearch(
            hosts="https://neteye2.test:9200",
            api_key=(self.API_KEY_ID, self.API_KEY_SECRET), 
            ca_certs=self.ROOT_CA_PATH
            )
        
        res = es.search(index="adult", query=query)
        print("Got %d Hits:" % res['hits']['total']['value'])
        for hit in res['hits']['hits']:
            print("%(age)s %(native_country)s: %(education)s" % hit["_source"])            


