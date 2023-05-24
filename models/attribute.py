from __future__ import annotations

from abc import ABC, abstractmethod

from datetime import datetime
from dateutil import parser

from models.config import Config
from models.gentree import GenTree



class Attribute(ABC):
    """ Class representing the metadata about one attribute in the partition

    Attributes        
        width                           for categorical attributes stores the number of leaf node, for numerical attribute stores the number range
        gen_value                       the current state of the generalization
        split_allowed                   0 if the partition cannot be split further along the attribute, 1 otherwise        
    """    

    def __init__(self, name: str, width: int, gen_value: str, split_allowed: bool = True):        
        self.name = name
        self.width = width
        self.gen_value = gen_value        
        self.split_allowed = split_allowed


    def get_normalized_width(self) -> float:
        return self.width * 1.0 / len(Config.attr_metadata[self.name])
    

    @abstractmethod
    def split(self) -> list[Attribute]:
        pass

    @abstractmethod
    def map_to_es_query(self) -> dict:
        pass


class HierarchicalAttribute(Attribute):        
    def split(self) -> list[Attribute]:
        node_to_split_at: GenTree = Config.attr_metadata[self.name].node(self.gen_value)
        
        return [
            HierarchicalAttribute(
                name=self.name,
                width=len(child),
                gen_value=child.value,
                split_allowed=bool(len(child.children))
                ) 
            for child in node_to_split_at.children
        ]


    def map_to_es_query(self) -> dict:
        current_node = Config.attr_metadata[self.name].node(self.gen_value)                

        return {"terms": {f"{self.name}": current_node.get_leaf_node_values()}}
    

class NumericalAttribute(Attribute):
    @abstractmethod
    def get_new_limits() -> list[(int, int)]:
        pass

    def split(self) -> list[NumericalAttribute]:
        return [
            (
                self.name, 
                new_max - new_min, 
                f"{new_min},{new_max}" if new_min != new_max else str(new_min), 
                new_min != new_max
            ) 
            for (new_min, new_max) in self.get_new_limits()
        ]
    
    
    def map_to_es_query(self) -> dict:
        range_min_and_max = self.gen_value.split(',')
        # If this is not a range ('20,30') any more, but a concrete number (20), simply return the number
        if len(range_min_and_max) <= 1:                    
            return {"term": {self.name: range_min_and_max[0]}}
        else:
            return {"range": { self.name: { "gte": range_min_and_max[0], "lte": range_min_and_max[1]}}}


class MondrianNumericalAttribute(NumericalAttribute):
    def set_limits(self, limits):
        self.limits = limits

    def get_new_limits(self) -> list[int]:        
        return self.limits
    
    def split(self) -> list[MondrianNumericalAttribute]:
        return [
            MondrianNumericalAttribute(
                name=attr_val_tuple[0], 
                width=attr_val_tuple[1], 
                gen_value=attr_val_tuple[2], 
                split_allowed=attr_val_tuple[3]) 
            for attr_val_tuple in super().split()]
    


class MondrianTimestampAttribute(MondrianNumericalAttribute):
    pass
    # def set_limits(self, limits):
    #     self.limits = [(parser.parse(min_value), parser.parse(max_value)) for (min_value, max_value) in limits]
    
    
    # def map_to_es_query(self) -> dict:
    #     range_min_and_max = [datetime.fromtimestamp(gen_val).isoformat() for gen_val in self.gen_value.split(',')]
    #     # If this is not a range ('20,30') any more, but a concrete number (20), simply return the number
    #     if len(range_min_and_max) <= 1:                    
    #         return {"term": {self.name: range_min_and_max[0]}}
    #     else:
    #         return {"range": { self.name: { "gte": range_min_and_max[0], "lte": range_min_and_max[1]}}}