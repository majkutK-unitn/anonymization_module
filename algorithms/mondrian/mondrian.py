from typing import Tuple

from interfaces.abstract_algorithm import AbstractAlgorithm
from interfaces.mondrian_api import MondrianAPI

from models.attribute import Attribute
from models.config import Config
from models.gentree import GenTree
from models.numrange import NumRange
from models.partition import Partition

from utils.config_processor import parse_config


class Mondrian(AbstractAlgorithm):
    def __init__(self, db_connector: MondrianAPI):
        self.db_connector = db_connector

        self.final_partitions : list[Partition] = []


    def get_normalized_width(self, partition: Partition, qid_name: str) -> float:    
        """ Return Normalized width of partition """        

        return partition.attributes[qid_name].width * 1.0 / len(Config.attr_metadata[qid_name])


    def choose_qid_name(self, partition: Partition) -> str:
        """ Choose QID with largest normlized width and return its index. """

        max_norm_width = -1
        qid_name: str = None

        for attr_name in partition.attributes.keys():
            if partition.attributes[attr_name].split_allowed:
                normalized_width = self.get_normalized_width(partition, attr_name)
            
                if normalized_width > max_norm_width:
                    max_norm_width = normalized_width
                    qid_name = attr_name

        if max_norm_width > 1:        
            raise Exception("Max normalized width is greater than 1")

        return qid_name


    def split_numerical_value(self, numeric_value: str, value_to_split_at: int, next_unique_value: int) -> Tuple[str, str]:
        """ Split numeric value along value_to_split_at and return sub ranges """

        range_min_and_max = numeric_value.split(',')
        # If this is not a range ('20,30') any more, but a concrete number (20), simply return the number
        if len(range_min_and_max) <= 1:
            return range_min_and_max[0], range_min_and_max[0]    
        else:
            min_value = int(range_min_and_max[0])
            max_value = int(range_min_and_max[1])
            # Create two new partitions using the [mix, value_to_split_at] and [value_to_split_at, max] new ranges
            if min_value == value_to_split_at:
                l_range = str(min_value)
            else:
                l_range = f"{min_value},{value_to_split_at}"
            if max_value == next_unique_value:
                r_range = str(max_value)
            else:
                r_range = f"{next_unique_value},{max_value}"
                
            return l_range, r_range


    def split_numerical_attribute(self, partition: Partition, qid_name: str) -> list[Partition]:
        """ Split numeric attribute by along the median, creating two new sub-partitions """

        sub_partitions: list[Partition] = []
        
        (median, next_unique_value) = self.db_connector.get_attribute_median_and_next_unique_value(partition.attributes, qid_name)
        (min_value, max_value) = self.db_connector.get_attribute_min_max(qid_name, partition.attributes)

        # As cuts along other dimensions are done, the min-max of the partition along other dimensions migth change and needs to be updated
        updated_width = max_value - min_value
        updated_gen_value: str

        if min_value == max_value:
            updated_gen_value = str(min_value)
        else:
            updated_gen_value = f"{min_value},{max_value}"

        # The same Attribute object should not be directly manipulated, as other Partitions might also rely on it. A fresh one must be created.
        partition.attributes[qid_name] = Attribute(updated_width, updated_gen_value)

        if median is None or next_unique_value is None:
            return []

        l_attributes = partition.attributes.copy()
        r_attributes = partition.attributes.copy()

        (l_gen_value, r_gen_value) = self.split_numerical_value(partition.attributes[qid_name].gen_value, median, next_unique_value)
        
        l_width = median - min_value    
        r_width = max_value - next_unique_value

        l_attributes[qid_name] = Attribute(l_width, l_gen_value)
        r_attributes[qid_name] = Attribute(r_width, r_gen_value)

        l_count = self.db_connector.get_document_count(l_attributes)
        r_count = self.db_connector.get_document_count(r_attributes)

        if l_count < Config.k or r_count < Config.k:
            return []

        sub_partitions.append(Partition(l_count, l_attributes))
        sub_partitions.append(Partition(r_count, r_attributes))

        return sub_partitions


    def split_categorical_attribute(self, partition: Partition, qid_name: str) -> list[Partition]:
        """ Split categorical attribute using generalization hierarchy """

        node_to_split_at: GenTree = Config.attr_metadata[qid_name].node(partition.attributes[qid_name].gen_value)

        # If the node (has no children, and thus) is a leaf, the partitioning is not possible
        if not len(node_to_split_at.children):
            return []
        
        sub_partitions: list[Partition] = []

        for child in node_to_split_at.children:
            generalized_attrs = partition.attributes.copy()
            generalized_attrs[qid_name] = Attribute(len(child), child.value)

            count_covered_by_child = self.db_connector.get_document_count(generalized_attrs)
            
            if count_covered_by_child == 0:
                continue

            if count_covered_by_child < Config.k:
                return []
            
            sub_partitions.append(Partition(count_covered_by_child, generalized_attrs))

        if sum(sub_p.count for sub_p in sub_partitions) != partition.count:    
            raise Exception("The number of items in the subpartitions is not equal to that of the original partition")    
    
        return sub_partitions


    def split_partition(self, partition: Partition, qid_name: str):
        """ Split partition and distribute records to different sub-partitions """

        if isinstance(Config.attr_metadata[qid_name], NumRange):
            return self.split_numerical_attribute(partition, qid_name)
        else:
            return self.split_categorical_attribute(partition, qid_name)


    def check_splitable(self, partition: Partition):
        """ Check if the partition can be further split while satisfying k-anonymity """

        # The sum of all the boolean values is True, if any of the attributes is splittable
        if partition.count >= 2 * Config.k and sum(map(lambda part: part.split_allowed, partition.attributes.values())):
            return True

        return False


    def anonymize(self, partition: Partition):
        """ Main procedure of Half_Partition. Recursively partition groups until not allowable. """
    
        # Close the EC, if not splittable any more
        if self.check_splitable(partition) is False:
            self.final_partitions.append(partition)
            return
    
        qid_name = self.choose_qid_name(partition)

        if qid_name == None:
            raise Exception("No QID was chosen in the choose_qid_name call")    

        sub_partitions = self.split_partition(partition, qid_name)
        if len(sub_partitions) == 0:
            # Close the attribute for this partition, as it cannot be split any more
            # The same Attribute object should not be directly manipulated, as other Partitions might also rely on it. A fresh one must be created.
            partition.attributes[qid_name] = Attribute(partition.attributes[qid_name].width, partition.attributes[qid_name].gen_value, False)
            self.anonymize(partition)
        else:
            for sub_p in sub_partitions:
                self.anonymize(sub_p)

    
    def set_up_the_first_partition(self):
        """ Reset all global variables """        

        attributes: dict[str, Attribute] = {}        
        
        for attr_name in Config.qid_names:
            root_node_or_num_range = Config.attr_metadata[attr_name]    
            attributes[attr_name] = Attribute(len(root_node_or_num_range), root_node_or_num_range.value)
            
        whole_partition = Partition(Config.size_of_dataset, attributes)        
        
        return whole_partition
    

    def initialize(self, config: dict[str, int|dict]):
        parse_config(config, self.db_connector)      

    
    def run(self, config: dict[str, int|dict]) -> bool:
        """
        Basic Mondrian for k-anonymity.
        This fuction support both numeric values and categoric values.
        For numeric values, each iterator is a mean split.
        For categoric values, each iterator is a split using the generalization hierarchies.
        The final result is returned in 2-dimensional list.
        """

        self.initialize(config)

        whole_partition = self.set_up_the_first_partition()        

        self.anonymize(whole_partition)

        if sum(map(lambda partition: partition.count, self.final_partitions)) != whole_partition.count:        
            raise Exception("Losing records during anonymization")

        return self.db_connector.push_ecs(self.final_partitions)
    

    def calculate_ncp(self):
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
