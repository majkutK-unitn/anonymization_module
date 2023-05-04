from models.attribute import Attribute
from models.gentree import GenTree
from models.numrange import NumRange


class Partition(object):
    """ Class representing one partition output by the Mondrian cuts

    Attributes
        count                               the number of items in the partition
        attributes                          key-value pair, where the key is the attribute name and the value is the state of attribute in the current anonymization process
    """
    attr_dict: dict[str, NumRange|GenTree]

    def __init__(self, count: int, attributes: dict[str, Attribute]):        
        self.count = count
        self.attributes = attributes

    # The number of records in partition
    def __len__(self):        
        return len(self.count)