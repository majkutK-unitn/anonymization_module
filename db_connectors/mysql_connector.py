from os import getenv

from typing import Tuple

from functools import reduce

import mysql.connector

import tqdm

from interfaces.datafly_api import DataflyAPI
from interfaces.mondrian_api import MondrianAPI

from models.attribute import Attribute
from models.config import Config
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

        return f"WHERE {' AND '.join([attr.map_to_sql_query() for attr in attributes.values()])}"


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
        interval_size = 100 / num_of_buckets            
        percentiles = [interval_size*i for i in range(1, num_of_buckets + 1)]
        
        bucket_upper_bounds = list(set([self.get_value_at_percentile(attr_name, None, Config.size_of_dataset, percentile) for percentile in percentiles]        ))
        bucket_upper_bounds.sort()

        min = self.get_attribute_min(attr_name, None)

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
    

    def map_partition_to_mysql_anon_record(self, partition: Partition) -> dict[str, dict|str]:
        return reduce(lambda acc, curr: acc | curr, [attr.map_to_sql_attribute() for attr in partition.attributes.values()])
    

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