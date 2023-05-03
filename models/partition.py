class Partition(object):
    """ Class representing one partition output by the Mondrian cuts

    Attributes
        count                               the number of items in the partition
        attribute_width_list                (list per QID) for categorical attributes stores the number of leaf node, for numerical attribute stores the number range
        attribute_generalization_list       (list per QID) the current state of the generalization
        allow                               (list per QID) 0 if the partition cannot be split further along the attribute, 1 otherwise
    """

    def __init__(self, count: int, attribute_width_list: list[int], attribute_generalization_list: list[str], qi_len: int):        
        self.count = count
        self.attribute_width_list = list(attribute_width_list)
        self.attribute_generalization_list = list(attribute_generalization_list)
        self.attribute_split_allowed_list = [1] * qi_len

    # The number of records in partition
    def __len__(self):        
        return len(self.count)