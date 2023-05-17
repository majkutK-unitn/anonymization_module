import json
import time

from algorithms.datafly.datafly import Datafly
from algorithms.mondrian.mondrian import Mondrian

from interfaces.abstract_algorithm import AbstractAlgorithm
from interfaces.abstract_api import AbstractAPI

from db_connectors.es_connector import EsConnector


def read_config(file_name: str) -> dict[str, int|dict]:
    config_file = open(file_name)
    config = json.load(config_file)
    config_file.close()

    return config
        

def wire_up(algorithm: str, db_connector: AbstractAPI) -> AbstractAlgorithm:
    if algorithm == "datafly":
        return Datafly(db_connector)        

    if algorithm == "mondrian":
        return Mondrian(db_connector)
    
    raise Exception(f"No algorithm implementation name '{algorithm}' exists!")
    

if __name__ == '__main__':
    config_file_path = "configs/adults_config.json"
    algorithm_name = "mondrian"

    assert algorithm_name in ["datafly", "mondrian"]

    config = read_config(config_file_path)
    es_connector = EsConnector()

    algorithm = wire_up(algorithm_name, es_connector)

    start_time = time.time()

    print(f"Running {algorithm_name} with k={config['k']}...")

    algorithm.run(config)
    
    exec_time = float(time.time() - start_time)

    ncp = algorithm.calculate_ncp()
        
    print("NCP %0.2f" % ncp + "%")
    print("Run for %0.2f" % exec_time + " seconds")
