from models.attribute import Attribute
from models.gentree import GenTree
from models.numrange import NumRange


class Partition(object):
    """ Class representing one partition output by the Mondrian cuts

    Attributes
        count                               the number of items in the partition
        attributes                          key-value pair, where the key is the attribute name and the value is the state of attribute in the current anonymization process
    """
    # TODO: rename to something more useful / at least capitalize
    attr_dict: dict[str, NumRange|GenTree]

    def __init__(self, count: int, attributes: dict[str, Attribute]):        
        self.count = count
        self.attributes = attributes