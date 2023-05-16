from os import getenv

import tqdm

from typing import Tuple

from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk

from models.attribute import Attribute
from models.gentree import GenTree
from models.numrange import NumRange
from models.partition import Partition
from models.config import Config

from interfaces.datafly_api import DataflyAPI
from interfaces.mondrian_api import MondrianAPI

# TODO:
#   (1) remove the int() casts so that the project can handle floats as well (with the adult dataset we only need ints)
class EsConnector(MondrianAPI, DataflyAPI):

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


    def map_docs_to_individual_anonymized_docs(self, original_docs: list, anon_doc_with_qids: dict[str, str]):
        ''' 
        For every original document, create an anonymized one 
            { ...qids, "sa_1": "a", "sa_2": "b" },
            { ...qids, "sa_1": "a", "sa_2": "d" },
            { ...qids, "sa_1": "a", "sa_2": "d" },
        '''

        for doc in original_docs:                
            yield anon_doc_with_qids | {sensitive_attr_name: doc[sensitive_attr_name][0] for sensitive_attr_name in Config.sensitive_attrs}


    def map_docs_to_array_of_unique_sa_combinations_per_partition(self, original_docs: list, anon_doc_with_qids: dict[str, str]):
        ''' 
        For every partition, create one document, with all sensitive attribute values mapped into one sensitive_attrs field 
            { 
                ...qids,
                "sensitive_attrs": [
                    { "sa_1": "a", "sa_2": "b", "count": 1},
                    { "sa_1": "a", "sa_2": "d", "count": 4}
                    { "sa_1": "c", "sa_2": "d", "count": 23}
                ]
            }
        '''
        accumulator = {}

        for doc in original_docs:                
            temp = {sensitive_attr_name: doc[sensitive_attr_name][0] for sensitive_attr_name in Config.sensitive_attrs}
            if str(temp) in accumulator:
                accumulator[str(temp)]["count"] += 1
            else:
                accumulator[str(temp)] = temp | {"count": 1}
        
        yield anon_doc_with_qids | {"sensitive_attrs": list(accumulator.values())}

    
    def map_docs_to_unique_sa_combinations_per_partition(self, original_docs: list, anon_doc_with_qids: dict[str, str]):
        ''' 
        For every unique sensitive attribute value combination, create a document with the count of documents with this signature
            { ...qids, "sa_1": "a", "sa_2": "b", "count": 1 },
            { ...qids, "sa_1": "a", "sa_2": "d", "count": 4 }, 
            { ...qids, "sa_1": "c", "sa_2": "d", "count": 23 }
        '''
        accumulator = {}

        for doc in original_docs:                
            temp = {sensitive_attr_name: doc[sensitive_attr_name][0] for sensitive_attr_name in Config.sensitive_attrs}
            if str(temp) in accumulator:
                accumulator[str(temp)]["count"] += 1
            else:
                accumulator[str(temp)] = temp | {"count": 1}

        for unique_sa_combination in list(accumulator.values()):
             yield anon_doc_with_qids | unique_sa_combination                

    
    def map_docs_to_sa_arrays_per_partition(self, original_docs: list, anon_doc_with_qids: dict[str, str]):
        ''' 
        For every partition, create one document, with all sensitive values dumped into an array
            { 
                ...qids,
                "sa_1": [ "a", "a", "c" ],
                "sa_2": [ "b", "d", "d" ]
            }
        '''

        accumulator = {sensitive_attr_name: [] for sensitive_attr_name in Config.sensitive_attrs}

        for doc in original_docs:
            for sensitive_attr_name in Config.sensitive_attrs:
                accumulator[sensitive_attr_name].append(doc[sensitive_attr_name][0])

        yield anon_doc_with_qids | accumulator


    def generate_anonymized_docs(self, partitions: list[Partition]):
        for partition in partitions:
            query = self.map_attributes_to_query(partition.attributes)
            res = self.es_client.search(index="adults", query=query, fields=Config.sensitive_attrs, _source=False, size=partition.count)
            original_docs = list(map(lambda hit: hit["fields"], res["hits"]["hits"]))            
            
            # TODO: map to ES data_types
            anon_doc_with_qids = {attr_name: attr.gen_value for attr_name, attr in partition.attributes.items()}            
            
            yield from self.map_docs_to_individual_anonymized_docs(original_docs, anon_doc_with_qids)



    def push_ecs(self, partitions: list[Partition]) -> bool:
        progress = tqdm.tqdm(unit="docs", total=Config.size_of_dataset)
        successes = 0
        for ok, action in streaming_bulk(
            client=self.es_client, index="adults_anon", actions=self.generate_anonymized_docs(partitions),
        ):
            progress.update(1)
            successes += ok
        print("Indexed %d/%d documents" % (successes, Config.size_of_dataset))


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


# ------------------------------
# >>    Mondrian API - BEGIN
# ------------------------------    

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
    

# ------------------------------
# <<    Mondrian API - END
# ------------------------------



# ------------------------------
# >>    DataFly API - BEGIN
# ------------------------------
    
    def spread_attribute_into_uniform_buckets(self, attr_name: str, num_of_buckets: int) -> list[NumRange]:
        interval_size = int(100 / num_of_buckets)
        percentiles = list(range(interval_size,100,interval_size))

        aggs = {            
            f"{attr_name}_percentiles": { "percentiles": { "field": attr_name, "percents": percentiles } }            
        }

        res = self.es_client.search(index=self.INDEX_NAME, size=0, aggs=aggs)

        bucket_upper_bounds = list(map(lambda x: int(x), res["aggregations"][f"{attr_name}_percentiles"]["values"].values()))
        min, max = self.get_attribute_min_max(attr_name)

        num_ranges: list[NumRange] = []

        for i, bound in enumerate(bucket_upper_bounds + [max]):
            if i == 0:
                num_ranges.append(NumRange(min, bound))                
                continue

            if bucket_upper_bounds[i-1] == bound:
                num_ranges.append(NumRange(bound, bound))
            else:
                num_ranges.append(NumRange(bucket_upper_bounds[i-1] + 1, bound))            

        return num_ranges
    

# ------------------------------
# <<    DataFly API - END
# ------------------------------



    def map_attributes_to_query(self, attributes: dict[str, Attribute]):
        must = []

        for attr_name in attributes.keys():
            node_or_range = Config.attr_metadata[attr_name]

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