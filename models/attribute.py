class Attribute(object):
    """ Class representing the metadata about one attribute in the partition

    Attributes        
        width                           for categorical attributes stores the number of leaf node, for numerical attribute stores the number range
        gen_value                       the current state of the generalization
        split_allowed                   0 if the partition cannot be split further along the attribute, 1 otherwise        
    """    

    def __init__(self, width: int, gen_value: str, split_allowed: bool = True):        
        self.width = width
        self.gen_value = gen_value        
        self.split_allowed = split_allowed