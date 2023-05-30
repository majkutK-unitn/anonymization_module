import functools

from models.attribute import Attribute
from models.gentree import GenTree
from models.numrange import NumRange


class Partition(object):
    """ Class representing one partition output by the Mondrian cuts

    Attributes
        count                               the number of items in the partition
        attributes                          key-value pair, where the key is the attribute name and the value is the state of attribute in the current anonymization process
    """            

    def __init__(self, count: int, attributes: dict[str, Attribute]):        
        self.count = count
        self.attributes = attributes

    def __str__(self) -> str:
        return functools.reduce(lambda a,b: f"{a}, {b}", map(lambda attr_name_and_value: f"'{attr_name_and_value[0]}': '{attr_name_and_value[1].get_gen_value()}'", self.attributes.items()))
