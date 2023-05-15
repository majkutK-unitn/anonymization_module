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
        self.partitions : list[Partition] = []        
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

        print("Done")

    
    def run(self) -> bool:
        self.generate_initial_partitions()
    
    def calculate_ncp(self) -> float:
        pass