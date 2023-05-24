import copy
import warnings

from interfaces.abstract_algorithm import AbstractAlgorithm
from interfaces.mondrian_api import MondrianAPI

from models.attribute import Attribute, HierarchicalAttribute, MondrianNumericalAttribute, MondrianTimestampAttribute
from models.config import Config
from models.gentree import GenTree
from models.numrange import NumRange
from algorithms.mondrian.mondrian_partition import MondrianPartition

from utils.config_processor import parse_config


class Mondrian(AbstractAlgorithm):
    db_connector: MondrianAPI
    PARTITION_UNDER_PROCESSING: MondrianPartition

    def __init__(self, db_connector: MondrianAPI):
        self.db_connector = db_connector

        self.final_partitions : list[MondrianPartition] = []


    def create_subpartitions_splitting_along(self, attribute: Attribute, partition: MondrianPartition) -> list[MondrianPartition]:
        subpartitions: list[MondrianPartition] = []

        if isinstance(attribute, MondrianNumericalAttribute):
            (median, next_unique_value) = self.db_connector.get_value_to_split_at_and_next_unique_value(attribute.name, partition)
            if median is None or next_unique_value is None:
                return []

            (min_value, max_value) = self.db_connector.get_attribute_min_max(attribute.name, partition.attributes)

            attribute.set_limits([(min_value, median), (next_unique_value, max_value)])

        split_attributes = attribute.split()
        
        for attr in split_attributes:            
            new_partition_attributes = partition.attributes.copy()
            new_partition_attributes[attr.name] = attr     

            subpartitions.append(MondrianPartition(self.db_connector.get_document_count(new_partition_attributes), new_partition_attributes))

        if sum(sub_p.count for sub_p in subpartitions) != partition.count:    
            raise Exception("The number of items in the subpartitions is not equal to that of the original partition")

        if sum(sub_p.count for sub_p in filter(lambda p: p.count >= Config.k, subpartitions)) != partition.count:
            return []

        return subpartitions

    

    def anonymize(self, partition: MondrianPartition):
        """ Main procedure of Half_MondrianPartition. Recursively partition groups until not allowable. """
    
        # Close the EC, if not splittable any more
        if not partition.check_if_splittable():
            self.final_partitions.append(partition)
            return                

        attr_to_split = partition.choose_attribute()
        subpartitions = self.create_subpartitions_splitting_along(attr_to_split, partition)        
                
        if len(subpartitions) == 0:
            # Close the attribute for this partition, as it cannot be split any more
            # The same Attribute object should not be directly manipulated, as other MondrianPartitions might also rely on it. A fresh one must be created.   
            copied_attr = copy.copy(attr_to_split)
            copied_attr.split_allowed = False
            partition.attributes[attr_to_split.name] = copied_attr
            self.anonymize(partition)
        else:
            if sum(sub_p.count for sub_p in subpartitions) != partition.count:    
                raise Exception("The number of items in the subpartitions is not equal to that of the original partition")
            
            for sub_p in subpartitions:
                self.anonymize(sub_p)

    
    def set_up_the_first_partition(self):
        """ Reset all global variables """        

        attributes: dict[str, Attribute] = {}
        
        for attr_name, value in Config.qids_config.items():
            root_node_or_num_range = Config.attr_metadata[attr_name]

            if value["type"] == "hierarchical":
                attributes[attr_name] = HierarchicalAttribute(attr_name, len(root_node_or_num_range), root_node_or_num_range.value)

            if value["type"] == "numerical":
                attributes[attr_name] = MondrianNumericalAttribute(attr_name, len(root_node_or_num_range), root_node_or_num_range.value)
            
            if value["type"] == "timestamp":
                attributes[attr_name] = MondrianTimestampAttribute(attr_name, len(root_node_or_num_range), root_node_or_num_range.value)
                        

        whole_partition_size = self.db_connector.get_document_count(attributes)
        if whole_partition_size != Config.size_of_dataset:
            warnings.warn(f"\n\n\n{'='*35}\tWARNING!\t{'='*35}\n\t>> The initial partition does not cover the entire dataset!\n{'='*91}\n\n")

        whole_partition = MondrianPartition(whole_partition_size, attributes)
        
        return whole_partition
    

    def initialize(self, config: dict[str, int|dict]):
        parse_config(config, self.db_connector)        

    
    def run(self, config: dict[str, int|dict]):
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

        return self.db_connector.push_partitions(self.final_partitions)        
