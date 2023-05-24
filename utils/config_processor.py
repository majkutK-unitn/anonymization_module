from interfaces.abstract_api import AbstractAPI

from models.attribute import Attribute
from models.config import Config
from models.gentree import GenTree
from models.numrange import NumRange
from models.partition import Partition

from utils.gen_hierarchy_parser import read_gen_hierarchies_from_json


def parse_config(config: dict[str, int|dict], db_connector: AbstractAPI):
    Config.k = config["k"]
    Config.qid_names: list[str] = list(config["qids"].keys())
    Config.sensitive_attr_names = config["sensitive_attributes"]

    Config.categorical_attr_config = dict(filter(lambda attr: "tree" in attr[1], config["qids"].items()))
    Config.numerical_attr_config = dict(filter(lambda attr: "tree" not in attr[1], config["qids"].items()))

    Config.qids_config = config["qids"]
    
    Config.gen_hiers = read_gen_hierarchies_from_json(Config.categorical_attr_config)

    Config.size_of_dataset = db_connector.get_document_count()

    _init_partitions_metadata(db_connector)


def _init_partitions_metadata(db_connector: AbstractAPI):    
    gen_hiers_and_num_ranges: dict[str, NumRange|GenTree] = Config.gen_hiers

    for attr_name in Config.qid_names:
        root_node_or_num_range: NumRange | GenTree

        if attr_name in Config.gen_hiers:
            root_node_or_num_range = Config.gen_hiers[attr_name]            
        else:            
            (min, max) = db_connector.get_attribute_min_max(attr_name)
            root_node_or_num_range = NumRange(min, max)
            gen_hiers_and_num_ranges[attr_name] = root_node_or_num_range
            
    Config.attr_metadata = gen_hiers_and_num_ranges