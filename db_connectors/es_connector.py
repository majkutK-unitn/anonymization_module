from os import getenv

import tqdm

from typing import Tuple

from elasticsearch import Elasticsearch, RequestError
from elasticsearch.helpers import streaming_bulk

from models.attribute import Attribute
from models.gentree import GenTree
from models.numrange import NumRange
from models.partition import Partition
from models.config import Config

from interfaces.datafly_api import DataflyAPI
from interfaces.mondrian_api import MondrianAPI


class EsConnector(MondrianAPI, DataflyAPI):

    def __init__(self):
        API_KEY_ID = getenv('ES_API_KEY_ID')
        API_KEY_SECRET = getenv('ES_API_KEY_SECRET')
        ROOT_CA_PATH = getenv('ROOT_CA_PATH')
        
        self.INDEX_NAME = getenv('INDEX_NAME')
        self.ANON_INDEX_NAME = f"{self.INDEX_NAME}-anonymized"

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
            yield anon_doc_with_qids | {sensitive_attr_name: doc[sensitive_attr_name][0] for sensitive_attr_name in Config.sensitive_attr_names}


    def map_docs_to_array_of_unique_sa_combinations_per_partition(self, original_docs: list, anon_doc_with_qids: dict[str, str]):
        ''' 
        For every partition, create one document, with all sensitive attribute values mapped into one sensitive_attr_names field 
            { 
                ...qids,
                "sensitive_attributes": [
                    { "sa_1": "a", "sa_2": "b", "count": 1},
                    { "sa_1": "a", "sa_2": "d", "count": 4}
                    { "sa_1": "c", "sa_2": "d", "count": 23}
                ]
            }
        '''
        sensitive_attributes = {}

        for doc in original_docs:                
            sens_attr = {sensitive_attr_name: doc[sensitive_attr_name][0] for sensitive_attr_name in Config.sensitive_attr_names}
            if str(sens_attr) in sensitive_attributes:
                sensitive_attributes[str(sens_attr)]["count"] += 1
            else:
                sensitive_attributes[str(sens_attr)] = sens_attr | {"count": 1}
        
        yield anon_doc_with_qids | {"sensitive_attributes": list(sensitive_attributes.values())}

    
    def map_docs_to_unique_sa_combinations_per_partition(self, original_docs: list, anon_doc_with_qids: dict[str, str]):
        ''' 
        For every unique sensitive attribute value combination, create a document with the count of documents with this signature
            { ...qids, "sa_1": "a", "sa_2": "b", "count": 1 },
            { ...qids, "sa_1": "a", "sa_2": "d", "count": 4 }, 
            { ...qids, "sa_1": "c", "sa_2": "d", "count": 23 }
        '''
        sensitive_attributes = {}

        for doc in original_docs:                
            sens_attr = {sensitive_attr_name: doc[sensitive_attr_name][0] for sensitive_attr_name in Config.sensitive_attr_names}
            if str(sens_attr) in sensitive_attributes:
                sensitive_attributes[str(sens_attr)]["count"] += 1
            else:
                sensitive_attributes[str(sens_attr)] = sens_attr | {"count": 1}

        for unique_sa_combination in list(sensitive_attributes.values()):
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

        sensitive_attributes = {sensitive_attr_name: [] for sensitive_attr_name in Config.sensitive_attr_names}

        for doc in original_docs:
            for sensitive_attr_name in Config.sensitive_attr_names:
                sensitive_attributes[sensitive_attr_name].append(doc[sensitive_attr_name][0])

        yield anon_doc_with_qids | sensitive_attributes


    def map_num_attr_to_es_range(self, attribute: Attribute):
        min_max = attribute.gen_value.split(",")

        return {
            "gte": min_max[0],
            "lte": min_max[1] if len(min_max) > 1 else min_max[0]
        }         
    

    def map_attributes_to_es_data_types(self, partition: Partition) -> dict[str, dict|str]:
        doc_with_qids = {}

        for attr_name, attribute in partition.attributes.items():
            if attr_name in Config.numerical_attr_config.keys():
                doc_with_qids[attr_name] = self.map_num_attr_to_es_range(attribute)
            else:
                doc_with_qids[attr_name] = attribute.gen_value

        return doc_with_qids


    def generate_anonymized_docs(self, partitions: list[Partition]):
        for partition in partitions:
            query = self.map_attributes_to_query(partition.attributes)

            res = self.es_client.search(index=self.INDEX_NAME, query=query, fields=Config.sensitive_attr_names, _source=False, size=partition.count)
            original_docs = list(map(lambda hit: hit["fields"], res["hits"]["hits"]))

            doc_with_qids = self.map_attributes_to_es_data_types(partition)
            
            yield from self.map_docs_to_individual_anonymized_docs(original_docs, doc_with_qids)


    def create_index(self):
        """Creates an index in Elasticsearch if one isn't already there."""

        try:
            self.es_client.indices.create(
                index=self.ANON_INDEX_NAME,
                mappings={"properties": 
                          {num_attr_name: {"type": "integer_range"} for num_attr_name in Config.numerical_attr_config.keys()} |
                          {cat_attr_name: {"type": "keyword"} for cat_attr_name in Config.categorical_attr_config.keys()}
                        }
            )
        except RequestError as exception:
            if exception.error != "resource_already_exists_exception":
                raise exception


    def push_ecs(self, partitions: list[Partition]) -> bool:
        self.create_index()

        progress = tqdm.tqdm(unit="docs", total=Config.size_of_dataset)
        successes = 0

        for ok, action in streaming_bulk(client=self.es_client, index=self.ANON_INDEX_NAME, actions=self.generate_anonymized_docs(partitions),):
            progress.update(1)
            successes += ok

        print("Indexed %d/%d documents" % (successes, Config.size_of_dataset))


    def get_document_count(self, attributes: dict[str, Attribute] = None) -> int:    
        query = None
        
        if attributes is not None:
            query = {"query": self.map_attributes_to_query(attributes)}

        res = self.es_client.count(index=self.INDEX_NAME, body=query)

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

    def get_median(self, attr_name: str, attributes: dict[str, Attribute]) -> int:
        query = self.map_attributes_to_query(attributes)

        aggs = {            
            f"{attr_name}_median": { "percentiles": { "field": attr_name, "percents": [ 50 ] }},            
        }

        res = self.es_client.search(index=self.INDEX_NAME, query=query, size=0, aggs=aggs)

        value = list(res["aggregations"][f"{attr_name}_median"]['values'].values())[0]

        return int(value)
    

    def get_unique_next_or_prev_value(self, direction: str, attributes: dict[str, Attribute], attr_name: str, central_value: int):
        assert direction in ["NEXT", "PREVIOUS"]

        (operator, func) = ("gt", "min") if direction == "NEXT" else ("lt", "max")

        query = self.map_attributes_to_query(attributes)
        query["bool"]["must"].append({"range": { attr_name: { operator: central_value } } })

        aggs = {f"{attr_name}_{func}_in_partition": { func: { "field": attr_name } }}

        res = self.es_client.search(index=self.INDEX_NAME, query=query, aggs=aggs, size=0)
        value = res["aggregations"][f"{attr_name}_{func}_in_partition"]['value']

        return int(value)


    def get_value_to_split_at_and_next_unique_value(self, attr_name: str, attributes: dict[str, Attribute]) -> Tuple[int, int]:
        """ Find the middle of the partition and the next unique value that follows the median """

        median = self.get_median(attr_name, attributes)
        (_, max_value) = self.get_attribute_min_max(attr_name, attributes)

        value_to_split_at: int
        next_unique_value: int

        if median == max_value:
            value_to_split_at = self.get_unique_next_or_prev_value("PREVIOUS", attributes, attr_name, max_value)
            next_unique_value = median
        else:
            value_to_split_at = median
            next_unique_value = self.get_unique_next_or_prev_value("NEXT", attributes, attr_name, median)

        return value_to_split_at, next_unique_value    
    

# ------------------------------
# <<    Mondrian API - END
# ------------------------------



# ------------------------------
# >>    DataFly API - BEGIN
# ------------------------------
    
    def spread_attribute_into_uniform_buckets(self, attr_name: str, num_of_buckets: int) -> list[NumRange]:
        interval_size = 100 / num_of_buckets            
        percentiles = [interval_size*i for i in range(1, num_of_buckets + 1)]

        aggs = {            
            f"{attr_name}_percentiles": { "percentiles": { "field": attr_name, "percents": percentiles } }            
        }

        res = self.es_client.search(index=self.INDEX_NAME, size=0, aggs=aggs)

        bucket_upper_bounds = list(set(map(lambda x: int(x), res["aggregations"][f"{attr_name}_percentiles"]["values"].values())))
        bucket_upper_bounds.sort()

        min, _ = self.get_attribute_min_max(attr_name)

        num_ranges: list[NumRange] = []

        for i, bound in enumerate(bucket_upper_bounds):
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