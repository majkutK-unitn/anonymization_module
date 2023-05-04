from typing import Tuple
from models.attribute import Attribute
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


    def get_document_count(self, attributes: dict[str, Attribute] = None):    
        query = None
        
        if attributes is not None:
            query = self.get_attribute_min_max(attributes)

        res = self.es_client.count(index="adults", body={"query": query})

        return res["count"]


    def get_median(self, attributes: dict[str, Attribute], attr_name: str) -> Tuple[str, str, str, str]:
        """ Find the middle of the partition

        Returns
        -------
        (str, str, str, str) > (median, value_after_median, min, max)
        """
        
        query = self.get_attribute_min_max(attributes)
        num_of_docs_in_partition = self.get_document_count(attributes)
        # At what percentage of the dataset is the value right after the median?
        percentile_for_value_after_median = (((num_of_docs_in_partition * 0.5) + 1) / num_of_docs_in_partition) * 100        

        aggs = {            
            f"{attr_name}_median": { "percentiles": { "field": attr_name, "percents": [ 50 ] }},
            f"{attr_name}_value_after_median": { "percentiles": { "field": attr_name, "percents": [ percentile_for_value_after_median ] }},
            f"{attr_name}_min": { "min": { "field": attr_name } },
            f"{attr_name}_max": { "max": { "field": attr_name } }            
        }

        res = self.es_client.search(index=self.INDEX_NAME, query=query, size=0, aggs=aggs)

        return list(res["aggregations"][f"{attr_name}_median"]['values'].values())[0], \
                list(res["aggregations"][f"{attr_name}_value_after_median"]['values'].values())[0], \
                res["aggregations"][f"{attr_name}_min"]['value'], \
                res["aggregations"][f"{attr_name}_max"]['value']
    
    
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

        res = self.es_client.search(index=self.INDEX_NAME, size=0, aggs=aggs)

        return res["aggregations"]["age_min"]['value'], res["aggregations"]["age_max"]['value']
    

    

    def map_attributes_to_query(self, attributes: dict[str, Attribute]):
        filter = []

        for attr_name in attributes.keys():
            node_or_range = Partition.attr_dict[attr_name]

            if isinstance(node_or_range, GenTree):
                # TODO: 
                #   (1) the covered_values list also contains the intermediate node values that are not needed as part of the query
                #   (2) simply putting all the values into a new term links them through ANDs in the query, thus it will have no results. Use should instead of filter
                pass
                #for leaf_node_value in node_or_range.covered_nodes.keys():                    
                #    filter.append({"term": {attr_name: leaf_node_value}})
            else:
                range_min_and_max = attributes[attr_name].gen_value.split(',')
                # If this is not a range ('20,30') any more, but a concrete number (20), simply return the number
                if len(range_min_and_max) <= 1:                    
                    filter.append({"term": {attr_name: range_min_and_max}})                    
                else:
                    filter.append({"range": {
                        attr_name: {
                            "gte": range_min_and_max[0],
                            "lte": range_min_and_max[1]
                            }}})
        return {
            "bool": {
                "filter": filter
            }    
        }
        