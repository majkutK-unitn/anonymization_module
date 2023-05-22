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
        
        self.INDEX_NAME = getenv('INDEX_NAME')
        self.ANON_INDEX_NAME = f"{self.INDEX_NAME}-anonymized"

        self.mysql_client = mysql.connector.connect(
            host="neteye2.test",
            user="anon_module",
            password="amRvaWVhaGRlYWlkaGVpdWFlZAo",
            database="anonymization"
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
        query = f"SELECT COUNT(*) FROM adults {where}"
        
        cursor.execute(query)
        count = cursor.fetchone()

        return count[0]
    
    
    def get_attribute_min_max(self, attr_name: str, attributes: dict[str, Attribute]) -> Tuple[int,int]:
        pass

    def spread_attribute_into_uniform_buckets(self, attr_name: str, num_of_buckets: int) -> list[NumRange]:
        pass

    def get_value_to_split_at_and_next_unique_value(self,  attr_name: str, attributes: dict[str, Attribute]) -> Tuple[int, int]:
        pass

    def push_partitions(self, partitions: list[Partition]):
        pass