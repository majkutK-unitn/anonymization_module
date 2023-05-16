from models.gentree import GenTree
from models.numrange import NumRange


class Config(object):
    """ Class representing one partition output by the Mondrian cuts

    Attributes
        k                                   determines the minimal size that each generated partition should have
        qid_names                           names of the attributes that are indirect or quasi-identifers
        sensitive_attrs                     names of the sensitive attributes
        categorical_attr_config             the input configuration per categorical attribute
        numerical_attr_config               the input configuration per numerical attribute
        gen_hiers                           parsed generalization hierarchies
        attr_metadata                       metadata about all quasi-identifier attributes
        size_of_dataset                     size of the entire, original dataset
    """
        
    k: int
    qid_names: list[str]
    sensitive_attrs: list[str]
    
    categorical_attr_config: dict[str, dict | int | str]
    numerical_attr_config: dict[str, dict | int | str]
    gen_hiers: dict[str, GenTree]
    attr_metadata: dict[str, NumRange|GenTree]

    size_of_dataset: int    