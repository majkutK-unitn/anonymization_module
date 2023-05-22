from os import getenv

from typing import Tuple

import mysql.connector

from interfaces.datafly_api import DataflyAPI
from interfaces.mondrian_api import MondrianAPI

from models.attribute import Attribute
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
    
    def map_attributes_to_query(attributes: dict[str, Attribute]) -> str:
        pass


    def get_document_count(self, attributes: dict[str, Attribute] = None) -> int:                
        if attributes is not None:
            pass

        cursor = self.mysql_client.cursor()
        cursor.execute("SELECT COUNT(*) FROM adults")
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