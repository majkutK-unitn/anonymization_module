from models.partition import Partition
from interfaces.mondrian_api import MondrianAPI

from elasticsearch import Elasticsearch

from os import getenv


class EsConnector(MondrianAPI):

    def __init__(self):
        API_KEY_ID = getenv('ES_API_KEY_ID')
        API_KEY_SECRET = getenv('ES_API_KEY_SECRET')
        ROOT_CA_PATH = getenv('ROOT_CA_PATH')
        
        self.INDEX_NAME = getenv('INDEX_NAME')
        self.es_client = Elasticsearch(
                hosts="https://neteye2.test:9200",
                api_key=(API_KEY_ID, API_KEY_SECRET), 
                ca_certs=ROOT_CA_PATH
            )    

    def search(self, query):                
        res = self.es_client.search(index=self.INDEX_NAME, query=query)                
        for hit in res['hits']['hits']:
            print("%(age)s %(native_country)s: %(education)s" % hit["_source"])

    
    def count(self, query):        
        res = self.es_client.count(index="adults", query=query)        
        print("Count: %d" % res['count'])        


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
    
    def get_number_of_nodes_covered(self, partition: Partition, qid_index: int, values: list[str]):
        pass
    
    
    def get_partition_count(self):
        query = {
            "range": {
                "age": {
                    "gte": 10,
                    "lte": 20
                    }
            }
        }
        
        return self.count(query)
    

    # TODO: 
    #   (1) reach the generalization hierarchy to be able to handle 
    #   (2) get access to the attribute names
    def map_partition_to_query(self, partition: Partition):
        filter = {}

        for generalized_value in partition.attr_gen_list:
            if Partition.is_qid_categorical:
                for leaf_node_value in ATTR_TREE.node(generalized_value).covered_nodes.keys():
                    filter['term'][name] = leaf_node_value
            else:
                range_min_and_max = generalized_value.split(',')
                # If this is not a range ('20,30') any more, but a concrete number (20), simply return the number
                if len(range_min_and_max) <= 1:
                    filter['term'][name] = generalized_value
                else:
                    filter['range'][name]['gte'] = range_min_and_max[0]
                    filter['range'][name]['lte'] = range_min_and_max[1]


        return {
            "query": {
                "bool": {
                    "filter": filter
                }                
            }
        }