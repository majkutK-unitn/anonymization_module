from typing import Tuple
from models.gentree import GenTree
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

    def push_ecs(ecs: list) -> bool:
        pass
        

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
    

    def get_attribute_min_max(self, attr_name: str) -> Tuple[int,int]:
        aggs = {            
            f"{attr_name}_min": { "min": { "field": attr_name } },
            f"{attr_name}_max": { "max": { "field": attr_name } },
        }

        res = self.es_client.search(index=self.INDEX_NAME, size=0, aggs=aggs)["aggregations"]

        return res["age_min"]['value'], res["age_max"]['value']
    

    

    def map_partition_to_query(self, partition: Partition):
        filter = {
            "term": {},
            "range": {}
        }

        for attr_name in partition.attributes.keys():
            node_or_range = Partition.attr_dict[attr_name]

            if isinstance(node_or_range, GenTree):
                for leaf_node_value in node_or_range.covered_nodes.keys():
                    filter['term'][attr_name] = leaf_node_value
            else:
                range_min_and_max = node_or_range.value.split(',')
                # If this is not a range ('20,30') any more, but a concrete number (20), simply return the number
                if len(range_min_and_max) <= 1:
                    filter['term'][attr_name] = range_min_and_max
                else:
                    filter['range'][attr_name] = {
                        "gte": range_min_and_max[0],
                        "lte": range_min_and_max[1]
                    }                    

        return {
            "query": {
                "bool": {
                    "filter": filter
                }                
            }
        }