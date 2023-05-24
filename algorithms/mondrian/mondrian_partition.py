from __future__ import annotations

from models.attribute import Attribute
from models.config import Config
from models.partition import Partition


class MondrianPartition(Partition):
    """ Extend the Partition class with algorithm-specific methods """

    def check_if_splittable(self) -> bool:
        if self.count >= 2 * Config.k and sum(map(lambda attr: attr.split_allowed, self.attributes.values())):
            return True

        return False
    

    def choose_attribute(self) -> Attribute:
        """ Choose QID with largest normlized width and return its index. """
        
        max_norm_width = -1
        chosen_attr: Attribute = None

        for attr_name, attr in self.attributes.items():
            if self.attributes[attr_name].split_allowed:
                normalized_width = attr.get_normalized_width()
            
                if normalized_width > max_norm_width:
                    max_norm_width = normalized_width
                    chosen_attr = attr

        if max_norm_width > 1:        
            raise Exception("Max normalized width is greater than 1")
        
        if chosen_attr == None:
            raise Exception("No QID was chosen in the choose_qid_name call")    

        return chosen_attr  