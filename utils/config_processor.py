from interfaces.abstract_api import AbstractAPI

from models.config import Config
from models.gentree import GenTree
from models.numrange import NumRange

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

    for attr_name, value in Config.qids_config.items():        
        if value["type"] == "numerical" or value["type"] == "timestamp":
            (min, max) = db_connector.get_attribute_min_max(attr_name)            
            gen_hiers_and_num_ranges[attr_name] = NumRange(min, max)
        
        if value["type"] == "ip":
            gen_hiers_and_num_ranges[attr_name] = NumRange(0, 1)
                        
    Config.attr_metadata = gen_hiers_and_num_ranges