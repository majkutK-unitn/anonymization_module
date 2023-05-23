from os import getenv

from typing import Tuple

import mysql.connector
import tqdm

from interfaces.datafly_api import DataflyAPI
from interfaces.mondrian_api import MondrianAPI

from models.attribute import Attribute
from models.config import Config
from models.gentree import GenTree
from models.numrange import NumRange
from models.partition import Partition


class MySQLConnector(MondrianAPI, DataflyAPI):

    def __init__(self):        
        MYSQL_HOST = getenv('MYSQL_HOST')
        MYSQL_USER = getenv('MYSQL_USER')
        MYSQL_PASSWORD = getenv('MYSQL_PASSWORD')
        MYSQL_DATABASE = getenv('MYSQL_DATABASE')        
        
        self.TABLE_NAME = getenv('MYSQL_TABLE_NAME')
        self.ANON_TABLE_NAME = f"{self.TABLE_NAME}_anonymized"

        self.mysql_client = mysql.connector.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE
        )
    
    def map_attributes_to_where_conditions(self, attributes: dict[str, Attribute]) -> str:
        if attributes is None:
            return ""
        
        queries_per_attribute: list[str] = []

        for attr_name in attributes.keys():
            node_or_range = Config.attr_metadata[attr_name]

            if isinstance(node_or_range, GenTree):                
                current_node = node_or_range.node(attributes[attr_name].gen_value)
                leaf_values_as_str = ",".join([f"'{s}'" for s in current_node.get_leaf_node_values()])
                queries_per_attribute.append(f"{attr_name} IN ({leaf_values_as_str})")
            else:
                range_min_and_max = attributes[attr_name].gen_value.split(',')
                # If this is not a range ('20,30') any more, but a concrete number (20), simply return the number
                if len(range_min_and_max) <= 1:
                    queries_per_attribute.append(f"{attr_name} = {range_min_and_max[0]}")                    
                else:
                    queries_per_attribute.append(f"({attr_name} >= {range_min_and_max[0]} AND {attr_name} <= {range_min_and_max[1]})")

        return f"WHERE {' AND '.join(queries_per_attribute)}"


    def get_document_count(self, attributes: dict[str, Attribute] = None) -> int:                
        where = self.map_attributes_to_where_conditions(attributes)

        cursor = self.mysql_client.cursor()
        query = f"SELECT COUNT(*) FROM {self.TABLE_NAME} {where}"
        
        cursor.execute(query)
        count = cursor.fetchone()

        return count[0]
    

    def get_aggregate(self, aggr_func: str, attr_name: str, attributes: dict[str, Attribute]) -> Tuple[int,int]:
        assert aggr_func in ["MIN", "MAX", "AVG", "SUM"]

        where = self.map_attributes_to_where_conditions(attributes)

        cursor = self.mysql_client.cursor()
        cursor.execute(f"SELECT {aggr_func}({attr_name}) FROM {self.TABLE_NAME} {where}")
        aggr_value = cursor.fetchone()

        return int(aggr_value[0])
    

    def get_attribute_min(self, attr_name: str, attributes: dict[str, Attribute]):
        return self.get_aggregate("MIN", attr_name, attributes)
    

    def get_attribute_max(self, attr_name: str, attributes: dict[str, Attribute]):
        return self.get_aggregate("MAX", attr_name, attributes)
        

    def get_attribute_min_max(self, attr_name: str, attributes: dict[str, Attribute] = None) -> Tuple[int,int]:
        return self.get_attribute_min(attr_name, attributes), self.get_attribute_max(attr_name, attributes)
    

    def get_value_at_percentile(self, attr_name: str, attributes: dict[str, Attribute], partition_size: int, percentile: float) -> int:
        if int(percentile) >= 100:
            return self.get_attribute_max(attr_name, attributes)

        where = self.map_attributes_to_where_conditions(attributes)
        index = int(partition_size * (percentile / 100))

        query = f"SELECT {attr_name} FROM {self.TABLE_NAME} {where} ORDER BY {attr_name} DESC LIMIT {index},1"

        cursor = self.mysql_client.cursor()
        cursor.execute(query)
        value_at_percentile = cursor.fetchone()

        return int(value_at_percentile[0])
    

    def get_median(self, attr_name: str, attributes: dict[str, Attribute], partition_size: int) -> int:
        return self.get_value_at_percentile(attr_name, attributes, partition_size, 50)

    
    def get_unique_next_or_prev_value(self, direction: str, attr_name, attributes: dict[str, Attribute], central_value: int):
        assert direction in ["NEXT", "PREVIOUS"]

        (operator, func) = (">", "MIN") if direction == "NEXT" else ("<", "MAX")

        where = self.map_attributes_to_where_conditions(attributes)

        query = f"SELECT {func}({attr_name}) FROM {self.TABLE_NAME} {where} AND {attr_name} {operator} {central_value}"

        cursor = self.mysql_client.cursor()
        cursor.execute(query)
        value = cursor.fetchone()

        return int(value[0])
    

    def get_value_to_split_at_and_next_unique_value(self,  attr_name: str, partition: Partition) -> Tuple[int, int]:        
        median = self.get_median(attr_name, partition.attributes, partition.count)
        max_value = self.get_attribute_max(attr_name, partition.attributes)

        value_to_split_at: int
        next_unique_value: int

        if median == max_value:
            value_to_split_at = self.get_unique_next_or_prev_value("PREVIOUS", attr_name, partition.attributes, max_value)
            next_unique_value = median
        else:
            value_to_split_at = median
            next_unique_value = self.get_unique_next_or_prev_value("NEXT", attr_name, partition.attributes, median)

        return value_to_split_at, next_unique_value
    

    def spread_attribute_into_uniform_buckets(self, attr_name: str, num_of_buckets: int) -> list[NumRange]:
        pass
    
        
    def map_numerical_attr_to_es_range(self, attr_name: str, attribute: Attribute):
        min_max = attribute.gen_value.split(",")

        return {
            f"{attr_name}_from": min_max[0],
            f"{attr_name}_to": min_max[1] if len(min_max) > 1 else min_max[0]
        }
    

    def map_partition_to_mysql_anon_record(self, partition: Partition) -> dict[str, dict|str]:
        doc_with_qids = {}

        for attr_name, attribute in partition.attributes.items():
            if attr_name in Config.numerical_attr_config.keys():
                doc_with_qids = doc_with_qids | self.map_numerical_attr_to_es_range(attr_name, attribute)
            else:
                doc_with_qids[attr_name] = attribute.gen_value

        return doc_with_qids
    

    def generate_anonymized_docs(self, partitions: list[Partition]):        
        for partition in partitions:
            where = self.map_attributes_to_where_conditions(partition.attributes)

            query = f"SELECT {','.join(Config.sensitive_attr_names)} FROM {self.TABLE_NAME} {where}"
            cursor = self.mysql_client.cursor()
            cursor.execute(query)
            sensitive_values_in_partition = cursor.fetchall()

            record_with_qids = self.map_partition_to_mysql_anon_record(partition)

            anon_records_in_partition = []

            for sens_values_per_record in sensitive_values_in_partition:
                anon_record = record_with_qids | {sensitive_attr_name: sens_values_per_record[i] for i, sensitive_attr_name in enumerate(Config.sensitive_attr_names)}
                                
                attr_names = ",".join(anon_record.keys())
                attr_value_placeholders =  ",".join(["%s"] * len(anon_record.values()))

                anon_records_in_partition.append(tuple(anon_record.values()))

            yield attr_names, attr_value_placeholders, anon_records_in_partition


    def push_partitions(self, partitions: list[Partition]):
        cursor = self.mysql_client.cursor()

        progress = tqdm.tqdm(unit="docs", total=Config.size_of_dataset)
        successes = 0

        for (attr_names, attr_value_placeholders, anon_records) in self.generate_anonymized_docs(partitions):
            query = f"INSERT INTO {self.ANON_TABLE_NAME} ({attr_names}) VALUES ({attr_value_placeholders})"
            cursor.executemany(query, anon_records)

            progress.update(cursor.rowcount)
            successes += cursor.rowcount

        self.mysql_client.commit()        

        print(f"Inserted {successes}/{Config.size_of_dataset} records.")