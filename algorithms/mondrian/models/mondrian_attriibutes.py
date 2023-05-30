from __future__ import annotations

from models.attribute import NumericalAttribute


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
    def split(self) -> list[MondrianTimestampAttribute]:
        return [
            MondrianTimestampAttribute(
                name=attr.name, 
                width=attr.width, 
                gen_value=attr.gen_value, 
                split_allowed=attr.split_allowed) 
            for attr in super().split()]


    def get_es_property_mapping(self):
        return {"type": "date_range"}