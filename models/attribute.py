from __future__ import annotations

from abc import ABC, abstractmethod

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


    def get_name(self):
        return self.name

    def get_width(self):
        return self.width

    def get_gen_value(self):
        return self.gen_value

    def get_split_allowed(self):
        return self.split_allowed

    def get_normalized_width(self) -> float:
        return self.get_width() * 1.0 / len(Config.attr_metadata[self.get_name()])
    

    @abstractmethod
    def split(self) -> list[Attribute]:
        pass

    @abstractmethod
    def map_to_es_query(self) -> dict:
        pass

    @abstractmethod
    def map_to_es_attribute(self) -> dict:
        pass

    @abstractmethod
    def get_es_property_mapping(self) -> dict:
        pass


class HierarchicalAttribute(Attribute):        
    def split(self) -> list[HierarchicalAttribute]:
        node_to_split_at: GenTree = Config.attr_metadata[self.get_name()].node(self.get_gen_value())
        
        return [
            HierarchicalAttribute(
                name=self.get_name(),
                width=len(child),
                gen_value=child.value,
                split_allowed=bool(len(child.children))
                ) 
            for child in node_to_split_at.children
        ]


    def map_to_es_query(self) -> dict:
        current_node = Config.attr_metadata[self.get_name()].node(self.gen_value)                

        return {"terms": {f"{self.get_name()}": current_node.get_leaf_node_values()}}
    

    def get_es_property_mapping(self):
        return {"type": "keyword"}
    

    def map_to_es_attribute(self):
        current_node = Config.attr_metadata[self.get_name()].node(self.get_gen_value())

        return current_node.get_leaf_node_values()


class RangeAttribute(Attribute):
    def set_limits(self, limits: list[str]):
        self.limits = limits

    def split(self) -> list[RangeAttribute]:
        return [
            (
                self.get_name(), 
                new_max - new_min, 
                f"{new_min},{new_max}" if new_min != new_max else str(new_min), 
                new_min != new_max
            ) 
            for (new_min, new_max) in self.limits
        ]
    
    
    def map_to_es_query(self) -> dict:
        range_min_and_max = self.get_gen_value().split(',')
        # If this is not a range ('20,30') any more, but a concrete number (20), simply return the number
        if len(range_min_and_max) <= 1:                    
            return {"term": {self.get_name(): range_min_and_max[0]}}
        else:
            return {"range": { self.get_name(): { "gte": range_min_and_max[0], "lte": range_min_and_max[1]}}}        
    
    
    def map_to_es_attribute(self):
        min_max = self.get_gen_value().split(",")

        return {
            "gte": min_max[0],
            "lte": min_max[1] if len(min_max) > 1 else min_max[0]
        }
    

class IntegerAttribute(RangeAttribute):
    def get_es_property_mapping(self):
        return {"type": "integer_range"}
    
    def split(self) -> list[IntegerAttribute]:
        return [
            IntegerAttribute(
                name=attr_val_tuple[0], 
                width=attr_val_tuple[1], 
                gen_value=attr_val_tuple[2], 
                split_allowed=attr_val_tuple[3]) 
            for attr_val_tuple in super().split()]
    

class DateAttribute(RangeAttribute):
    def get_es_property_mapping(self):
        return {"type": "date_range"}
    
    def split(self) -> list[DateAttribute]:
        return [
            DateAttribute(
                name=attr_val_tuple[0], 
                width=attr_val_tuple[1], 
                gen_value=attr_val_tuple[2], 
                split_allowed=attr_val_tuple[3]) 
            for attr_val_tuple in super().split()]
    

class TimestampInMsAttribute(DateAttribute):
    def __init__(self, name: str, width: int, gen_value: str, split_allowed: bool = True):
        self.num_attr = IntegerAttribute(name, width, gen_value, split_allowed)        

    def get_name(self):
        return self.num_attr.get_name()

    def get_width(self):
        return self.num_attr.get_width()

    def get_gen_value(self):
        return self.num_attr.get_gen_value()

    def get_split_allowed(self):
        return self.num_attr.get_split_allowed()
    
    def set_limits(self, limits):
        self.num_attr.set_limits(limits)

    def split(self) -> list[TimestampInMsAttribute]:
        return [
            TimestampInMsAttribute(
                name=attr.get_name(), 
                width=attr.get_width(), 
                gen_value=attr.get_gen_value(), 
                split_allowed=attr.get_split_allowed()) 
            for attr in self.num_attr.split()]
    


class IpAttribute(RangeAttribute):

    def __init__(self, name: str, split_allowed: bool = True, segments: list[int] = [0, 0, 0, 0], mask: int = 0):        
        super().__init__(name, 1, f"{segments[0]}.{segments[1]}.{segments[2]}.{segments[3]}/{mask}", split_allowed)
        self.ip_segments = segments
        self.mask = mask


    def split(self) -> list[Attribute]:
        index_of_segment_change = int(self.mask / 8)
        index_of_bit_in_segment_to_change = 7 - (self.mask % 8)

        newly_split_segments = [
            self.ip_segments.copy(),
            [s if i != index_of_segment_change else s + pow(2, index_of_bit_in_segment_to_change) for (i, s) in enumerate(self.ip_segments)]
        ]

        return [
            IpAttribute(
                name=self.get_name(),                
                split_allowed=(self.mask != 31),
                segments=new_s,
                mask = self.mask+1
                )
            for new_s in newly_split_segments
        ]
    

    def get_es_property_mapping(self):
        return {"type": "ip_range"}
    

    def map_to_es_attribute(self):
        return self.get_gen_value()