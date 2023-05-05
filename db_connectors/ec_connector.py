from os import getenv

from typing import Tuple

from elasticsearch import Elasticsearch

from models.attribute import Attribute
from models.gentree import GenTree
from models.partition import Partition
from interfaces.mondrian_api import MondrianAPI

# TODO:
#   (1) implement push_ecs()
#   (2) refactor map_attributes_to_query
#   (3) remove the int() casts so that the project can handle floats as well (with the adult dataset we only need ints)
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


    def get_document_count(self, attributes: dict[str, Attribute] = None) -> int:    
        query = None
        
        if attributes is not None:
            query = {"query": self.map_attributes_to_query(attributes)}

        res = self.es_client.count(index="adults", body=query)

        return int(res["count"])
    
    
    def get_attribute_min_max(self, attr_name: str, attributes: dict[str, Attribute] = None) -> Tuple[int,int]:
        query = None
        
        if attributes is not None:
            query = self.map_attributes_to_query(attributes)

        aggs = {            
            f"{attr_name}_min": { "min": { "field": attr_name } },
            f"{attr_name}_max": { "max": { "field": attr_name } },
        }

        res = self.es_client.search(index=self.INDEX_NAME, size=0, query=query, aggs=aggs)

        return int(res["aggregations"][f"{attr_name}_min"]['value']), int(res["aggregations"][f"{attr_name}_max"]['value'])


    def get_attribute_median_and_next_unique_value(self, attributes: dict[str, Attribute], attr_name: str) -> Tuple[int, int]:
        """ Find the middle of the partition and the next unique value that follows the median """
        
        median_query = self.map_attributes_to_query(attributes)        

        median_aggs = {            
            f"{attr_name}_median": { "percentiles": { "field": attr_name, "percents": [ 50 ] }},            
        }

        median_res = self.es_client.search(index=self.INDEX_NAME, query=median_query, size=0, aggs=median_aggs)

        if list(median_res["aggregations"][f"{attr_name}_median"]['values'].values())[0] is None:
            return None, None

        median = int(list(median_res["aggregations"][f"{attr_name}_median"]['values'].values())[0])

        next_unique_query = median_query.copy()
        next_unique_query["bool"]["must"].append({"range": {
                        attr_name: {
                            "gt": median
                            }}
        })

        next_unique_aggs = {            
            f"{attr_name}_min_in_partition": { "min": { "field": attr_name } },            
        }

        next_unique_res = self.es_client.search(index=self.INDEX_NAME, query=next_unique_query, size=0, aggs=next_unique_aggs)

        next_unique = None if next_unique_res["aggregations"][f"{attr_name}_min_in_partition"]['value'] is None else int(next_unique_res["aggregations"][f"{attr_name}_min_in_partition"]['value'])

        return median, next_unique
    

    def map_attributes_to_query(self, attributes: dict[str, Attribute]):
        must = []

        for attr_name in attributes.keys():
            node_or_range = Partition.attr_dict[attr_name]

            if isinstance(node_or_range, GenTree):
                leaf_values = []
                current_node = node_or_range.node(attributes[attr_name].gen_value)
                for covered_node in current_node.covered_nodes.values():
                    # Only filter for leaf values, as the intermediate ones are not in present in the dataset, they should not be part of the queries
                    if not covered_node.children:
                        leaf_values.append(covered_node.value)

                must.append({"terms": {f"{attr_name}.keyword": leaf_values}})
            else:
                range_min_and_max = attributes[attr_name].gen_value.split(',')
                # If this is not a range ('20,30') any more, but a concrete number (20), simply return the number
                if len(range_min_and_max) <= 1:                    
                    must.append({"term": {attr_name: range_min_and_max[0]}})                    
                else:
                    must.append({"range": {
                        attr_name: {
                            "gte": range_min_and_max[0],
                            "lte": range_min_and_max[1]
                            }}})
        return {
            "bool": {
                "must": must
            }    
        }