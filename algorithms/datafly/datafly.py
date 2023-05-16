from interfaces.abstract_algorithm import AbstractAlgorithm
from interfaces.datafly_api import DataflyAPI

from models.attribute import Attribute
from models.config import Config
from models.gentree import GenTree
from models.numrange import NumRange
from models.partition import Partition

from utils.config_processor import parse_config


class Datafly(AbstractAlgorithm):
    def __init__(self, db_connector: DataflyAPI):
        self.db_connector = db_connector        
        self.final_partitions : list[Partition] = []


    def combine_attribute_with_existing_partitions(self, existing_partitions: list[dict[str, Attribute]], attr_name: str, range_or_node: GenTree | NumRange):
        new_partitions: list[dict[str, Attribute]] = []

        for partition_existing in existing_partitions:
            partition_new = partition_existing.copy()
            partition_new[attr_name] = Attribute(len(range_or_node), range_or_node.value)
            new_partitions.append(partition_new)

        return new_partitions
    

    def generate_new_partition_combinations(self, existing_partitions: list[dict[str, Attribute]], attr_name: str, nodes_or_ranges: list[GenTree | NumRange]):
        new_partitions_accumulator: list[dict[str, Attribute]] = []

        for node_or_range in nodes_or_ranges:
            new_partitions_accumulator += self.combine_attribute_with_existing_partitions(existing_partitions, attr_name, node_or_range)

        return new_partitions_accumulator


    def generate_initial_partitions(self):
        temp_partitions: list[dict[str, Attribute]] = [{}]

        for attr_name, value in Config.numerical_attr_config.items():
            if value["datafly_num_of_buckets"] > 0:
                num_ranges = self.db_connector.spread_attribute_into_uniform_buckets(attr_name, value["datafly_num_of_buckets"])

                temp_partitions = self.generate_new_partition_combinations(temp_partitions, attr_name, num_ranges)
                

        for attr_name, value in Config.categorical_attr_config.items():
            if value["datafly_init_level"] > 0:
                nodes = Config.gen_hiers[attr_name].nodes_on_level(value["datafly_init_level"])

                temp_partitions = self.generate_new_partition_combinations(temp_partitions, attr_name, nodes)

        self.get_partition_counts(temp_partitions)


    def get_partition_counts(self, attributes_of_init_partitions: list[dict[str, Attribute]]):                
        for attributes in attributes_of_init_partitions:
            count = self.db_connector.get_document_count(attributes)
            if count != 0:
                self.final_partitions.append(Partition(count, attributes))

    
    def merge_generalized_partitions(self, partition: Partition, attr_name: str, new_attribute: Attribute, unique_values: dict[str, Partition]):
        partition.attributes[attr_name] = new_attribute

        if str(partition) in unique_values:
            unique_values[str(partition)].count += partition.count
        else:
            unique_values[str(partition)] = partition


    def generalize_numerical_attr(self, attr_name: str, unique_values: dict[str, Partition]):
        """ Merge adjacent partitions together. If there is an odd number of partitions, leave the last one as is. """

        attr_values = list(set(map(lambda p: p.attributes[attr_name].gen_value, self.final_partitions)))
        # An ordered list is required, as it is the adjacent partitions that are meant to be merged
        attr_values.sort()
        # key: old attribute values, value: the merged attribute values
        old_to_new_ranges: dict[str, Attribute] = {}

        for i in range(int(len(attr_values) / 2)):
            lower = attr_values[i].split(",")
            higher = attr_values[i + 1].split(",")
            
            min_val = int(lower[0])
            max_val = int(higher[1] if len(higher) > 1 else higher[0])

            gen_value = f"{min_val},{max_val}"
            width = max_val - min_val
            new_attribute = Attribute(width, gen_value)

            old_to_new_ranges[attr_values[2*i]] = new_attribute
            old_to_new_ranges[attr_values[2*i + 1]] = new_attribute

        if len(attr_values) % 2 == 1:
            gen_value = attr_values[-1]
            min_max = gen_value.split(",")
            width = 0 if len(gen_value) == 1 else int(min_max[1]) - int(min_max[0])
            old_to_new_ranges[gen_value] = Attribute(width, gen_value)


        for partition in self.final_partitions:            
            new_attribute = old_to_new_ranges[partition.attributes[attr_name].gen_value]
            self.merge_generalized_partitions(partition, attr_name, new_attribute, unique_values)


    def generalize_categorical_attr(self, attr_name: str, unique_values: dict[str, Partition]):
        """ Step one level up in the hierarchy tree """

        root = Config.attr_metadata[attr_name]

        for partition in self.final_partitions:
            new_attribute: Attribute
            current_node = root.node(partition.attributes[attr_name].gen_value)

            # The hierarchy trees are not necessarily balanced: in some cases one value might already have been generalized to the root value, therefore it is kept in its original form
            if current_node.ancestors:
                parent_node = current_node.ancestors[0]
                new_attribute = Attribute(len(parent_node), parent_node.value)
            else:
                new_attribute = partition.attributes[attr_name]
            
            self.merge_generalized_partitions(partition, attr_name, new_attribute, unique_values)


    def generalize(self) -> list[Partition]:
        attr_with_most_distinct = ("", -1)

        for attr_name in self.final_partitions[0].attributes.keys():
            distinct_value_count = len(set(map(lambda p: p.attributes[attr_name].gen_value, self.final_partitions)))
            if attr_with_most_distinct[1] < distinct_value_count:
                attr_with_most_distinct = (attr_name, distinct_value_count)

        unique_values: dict[str, Partition] = {}

        if attr_with_most_distinct[0] in Config.numerical_attr_config.keys():
            self.generalize_numerical_attr(attr_with_most_distinct[0], unique_values)
        else:
            self.generalize_categorical_attr(attr_with_most_distinct[0], unique_values)

        return list(unique_values.values())
    

    def initialize(self, config: dict[str, int|dict]):
        parse_config(config, self.db_connector)     

    
    def run(self, config: dict[str, int|dict]) -> bool:
        self.initialize(config)
        self.generate_initial_partitions()
        
        while sum(map(lambda x: x.count, filter(lambda x: x.count < Config.k, self.final_partitions))) > Config.k:
            self.final_partitions = self.generalize()

        not_generalized_attributes: dict[str, Attribute] = {}
        for attr_name in Config.qid_names:
            if attr_name not in self.final_partitions[0].attributes.keys():
                node_or_range = Config.attr_metadata[attr_name]                
                not_generalized_attributes[attr_name] = Attribute(len(node_or_range), node_or_range.value)

        for partition in self.final_partitions:
            partition.attributes.update(not_generalized_attributes)        

        return self.db_connector.push_ecs(self.final_partitions)
    

    def get_normalized_width(self, partition: Partition, qid_name: str) -> float:    
        """ Return Normalized width of partition """        

        return partition.attributes[qid_name].width * 1.0 / len(Config.attr_metadata[qid_name])
    
    
    def calculate_ncp(self) -> float:
        ncp = 0.0

        for partition in self.final_partitions:
            r_ncp = 0.0
            for attr_name in Config.qid_names:
                r_ncp += self.get_normalized_width(partition, attr_name)

            r_ncp *= partition.count
            ncp += r_ncp

        # covert to NCP percentage
        ncp /= len(Config.qid_names)
        ncp /= Config.size_of_dataset
        ncp *= 100

        return ncp