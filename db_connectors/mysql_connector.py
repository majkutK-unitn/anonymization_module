from os import getenv

from typing import Tuple

import mysql.connector

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
        self.ANON_TABLE_NAME = f"{self.TABLE_NAME}-anonymized"

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
    
    
    def get_attribute_min_max(self, attr_name: str, attributes: dict[str, Attribute] = None) -> Tuple[int,int]:
        where = self.map_attributes_to_where_conditions(attributes)

        cursor = self.mysql_client.cursor()
        cursor.execute(f"SELECT MIN({attr_name}) FROM {self.TABLE_NAME} {where}")
        min_value = cursor.fetchone()
        cursor.execute(f"SELECT MAX({attr_name}) FROM {self.TABLE_NAME} {where}")
        max_value = cursor.fetchone()

        return int(min_value[0]), int(max_value[0])
    

    def get_median(self, attr_name: str, attributes: dict[str, Attribute], count: int) -> int:
        where = self.map_attributes_to_where_conditions(attributes)
        middle_index = int(count*0.5)

        query = f"SELECT {attr_name} FROM {self.TABLE_NAME} {where} ORDER BY {attr_name} DESC LIMIT {middle_index},1"

        cursor = self.mysql_client.cursor()
        cursor.execute(query)
        median = cursor.fetchone()

        return int(median[0])
    
    def get_unique_next_or_prev_value(self, direction: str, attributes: dict[str, Attribute], attr_name: str, central_value: int):
        assert direction in ["NEXT", "PREVIOUS"]

        (operator, func) = (">", "MIN") if direction == "NEXT" else ("<", "MAX")

        where = self.map_attributes_to_where_conditions(attributes)

        query = f"SELECT {func}({attr_name}) FROM {self.TABLE_NAME} {where} AND {attr_name} {operator} {central_value}"

        cursor = self.mysql_client.cursor()
        cursor.execute(query)
        value = cursor.fetchone()

        return int(value[0])
    
    

    def get_value_to_split_at_and_next_unique_value(self,  attr_name: str, attributes: dict[str, Attribute], partition_count: int) -> Tuple[int, int]:
        median = self.get_median(attr_name, attributes, partition_count)
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
    

    def spread_attribute_into_uniform_buckets(self, attr_name: str, num_of_buckets: int) -> list[NumRange]:
        pass

    

    def push_partitions(self, partitions: list[Partition]):
        pass