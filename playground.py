import json
from algorithms.datafly.datafly import Datafly

from db_connectors.es_connector import EsConnector

from models.attribute import Attribute
from models.gentree import GenTree
from models.numrange import NumRange
from models.partition import Partition

from utils.gen_hierarchy_parser import read_gen_hierarchies_from_json, read_gen_hierarchies_from_text


ES_CONNECTOR = EsConnector()


def random_testing():
    connector = EsConnector()
    #connector.search({"term": {"_id": {"value": "CTdo4IcBJ6zU8THOrfFz"}}})    
    #print(connector.get_attribute_min_max("age"))
    #print(connector.get_document_count())

    x = read_gen_hierarchies_from_text(["marital_status", "workclass"])
      
    config_file = open('configs/adults_config.json')
    config = json.load(config_file)
    
    y = read_gen_hierarchies_from_json(config['qids'])
        
    config_file.close()

    Partition.ATTR_METADATA = y
    Partition.ATTR_METADATA["age"] = NumRange(10, 100)

    partition = Partition(21, {
        "age": Attribute(20, "30,50", True),
        "marital_status": Attribute(2, "Married", True)
    })

    z = connector.map_attributes_to_query(partition.attributes)

    #print(connector.get_median(partition.attributes, "education_num"))
    print(connector.get_document_count(partition.attributes))
    #connector.get_partition_count()

    print("DONE")


def __test__map_attributes_to_query():
    connector = EsConnector()

    config_file = open('configs/adults_config.json')
    config = json.load(config_file)
    
    y = read_gen_hierarchies_from_json(config['qids'])
        
    config_file.close()

    Partition.ATTR_METADATA = y
    Partition.ATTR_METADATA["age"] = NumRange(10, 100)

    partition = Partition(21, {
        "age": Attribute(20, "10,20", True),
        "marital_status": Attribute(4, "*", True)
    })

    query = connector.map_attributes_to_query(partition.attributes)

    print("DONE")


def run_anonymization():
    # qid_names = ["age", "education_num", "workclass", "marital_status", "occupation", "race", "sex", "native_country", "relationship"]
    qid_names = ["age", "education_num", "workclass", "marital_status", "occupation", "race", "sex", "native_country"]
    gen_hiers = read_gen_hierarchies_from_text(qid_names[2:])    
    k = 10

    print("K=", k)
    print("Mondrian")
    #result, eval_result = mondrian(gen_hiers, qid_names, k)    
    #print("NCP %0.2f" % eval_result[0] + "%")
    #print("Running time %0.2f" % eval_result[1] + " seconds")


def __test__spread_attribute_values():
    ES_CONNECTOR.spread_attribute_into_uniform_buckets("age", 20)


def __test__datafly_init():
    config_file = open('configs/adults_config.json')
    config = json.load(config_file)
    config_file.close()
    datafly = Datafly(ES_CONNECTOR)
    datafly.run(config)
    

__test__datafly_init()