from interfaces.abstract_api import AbstractAPI

from models.attribute import Attribute
from models.gentree import GenTree
from models.numrange import NumRange
from models.partition import Partition

from utils.gen_hierarchy_parser import read_gen_hierarchies_from_json


def parse_config(config: dict[str, int|dict], db_connector: AbstractAPI):    
    k = config["k"]
    qid_names: list[str] = list(config["attributes"].keys())

    cat_attrs_from_config = dict(filter(lambda attr: "tree" in attr[1], config['attributes'].items()))
    gen_hiers = read_gen_hierarchies_from_json(cat_attrs_from_config)

    size_of_dataset = db_connector.get_document_count()

    _init_partitions_metadata(qid_names, gen_hiers, db_connector)

    return (k, qid_names, gen_hiers, size_of_dataset)


def _init_partitions_metadata(qid_names, gen_hiers, db_connector: AbstractAPI):    
    gen_hiers_and_num_ranges: dict[str, NumRange|GenTree] = gen_hiers

    for attr_name in qid_names:
        root_node_or_num_range: NumRange | GenTree

        if attr_name in gen_hiers:
            root_node_or_num_range = gen_hiers[attr_name]            
        else:            
            (min, max) = db_connector.get_attribute_min_max(attr_name)
            root_node_or_num_range = NumRange(min, max)
            gen_hiers_and_num_ranges[attr_name] = root_node_or_num_range
            
    Partition.ATTR_METADATA = gen_hiers_and_num_ranges