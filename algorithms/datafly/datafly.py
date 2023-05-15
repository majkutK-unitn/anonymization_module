from interfaces.abstract_algorithm import AbstractAlgorithm
from interfaces.datafly_api import DataflyAPI
from models.attribute import Attribute

from models.gentree import GenTree
from models.numrange import NumRange
from models.partition import Partition

from utils.read_gen_hierarchies import read_gen_hierarchies_from_config_v2


class Datafly(AbstractAlgorithm):
    def __init__(self, db_connector: DataflyAPI, config: dict[str, int|dict]):
        self.db_connector = db_connector
        self.k: int = config["k"]
        self.qid_names: list[str] = config["attributes"].keys()        
        self.final_partitions : list[Partition] = []        
        self.size_of_dataset: int = None
        self.numerical_attr_config = {}
        self.categorical_attr_config = {}

        for key, value in config['attributes'].items():
            if "tree" in value:
                if value["datafly_init_level"] != 0:
                    self.categorical_attr_config[key] = value
            else:
                self.numerical_attr_config[key] = value

        self.gen_hiers: dict[str, GenTree] = read_gen_hierarchies_from_config_v2(self.categorical_attr_config)

        Partition.attr_dict = self.gen_hiers.copy()
        for num_attr_name in self.numerical_attr_config.keys():
            (min, max) = self.db_connector.get_attribute_min_max(num_attr_name)
            num_range = NumRange(min, max)
            Partition.attr_dict[num_attr_name] = num_range


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

        for attr_name, value in self.numerical_attr_config.items():
            num_ranges = self.db_connector.spread_attribute_into_uniform_buckets(attr_name, value["datafly_num_of_buckets"])

            temp_partitions = self.generate_new_partition_combinations(temp_partitions, attr_name, num_ranges)
                

        for attr_name, value in self.categorical_attr_config.items():            
            nodes = self.gen_hiers[attr_name].nodes_on_level(value["datafly_init_level"])

            temp_partitions = self.generate_new_partition_combinations(temp_partitions, attr_name, nodes)

        return temp_partitions


    def get_partition_counts(self):
        attributes_of_init_partitions = self.generate_initial_partitions()
        
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


    def  generalize_numerical_attr(self, attr_name: str, unique_values: dict[str, Partition]):
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

        root = Partition.attr_dict[attr_name]

        for partition in self.final_partitions:
            current_node = root.node(partition.attributes[attr_name].gen_value)
            parent_node = current_node.ancestors[0]

            new_attribute = Attribute(len(parent_node), parent_node.value)
            self.merge_generalized_partitions(partition, attr_name, new_attribute, unique_values)


    def generalize(self) -> list[Partition]:
        attr_with_most_distinct = ("", -1)

        for attr_name in self.final_partitions[0].attributes.keys():
            distinct_value_count = len(set(map(lambda p: p.attributes[attr_name].gen_value, self.final_partitions)))
            if attr_with_most_distinct[1] < distinct_value_count:
                attr_with_most_distinct = (attr_name, distinct_value_count)

        unique_values: dict[str, Partition] = {}

        if attr_with_most_distinct[0] in self.numerical_attr_config.keys():
            self.generalize_numerical_attr(attr_with_most_distinct[0], unique_values)
        else:
            self.generalize_categorical_attr(attr_with_most_distinct[0], unique_values)

        return list(unique_values.values())

    
    def run(self) -> bool:
        self.get_partition_counts()
        while sum(map(lambda x: x.count, filter(lambda x: x.count < self.k, self.final_partitions))) > self.k:
            self.final_partitions = self.generalize()

        return self.final_partitions
    
    def calculate_ncp(self) -> float:
        pass