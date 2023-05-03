class Partition(object):
    """Class representing one partition output by the Mondrian cuts   
    - self.count: the number of items in the partition
    - lists that store for each QID, under the index for the corresponding attribute,
        - self.attribute_width_list
            - for categorical attributes: the number of leaf node
            - for numerical attribute: the number range
        - self.attribute_generalization_list: the current state of the generalization per attribute
        - self.allow: 0 if the partition cannot be split further along the attribute, 1 otherwise
    """

    def __init__(self, count, attribute_width_list, attribute_generalization_list, qi_len):        
        self.count = count
        self.attribute_width_list = list(attribute_width_list)
        self.attribute_generalization_list = list(attribute_generalization_list)
        self.attribute_split_allowed_list = [1] * qi_len

    # The number of records in partition
    def __len__(self):        
        return len(self.count)