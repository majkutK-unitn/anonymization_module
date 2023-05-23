import json
import time

from algorithms.datafly.datafly import Datafly
from algorithms.mondrian.mondrian import Mondrian

from interfaces.abstract_algorithm import AbstractAlgorithm
from interfaces.abstract_api import AbstractAPI

from db_connectors.es_connector import EsConnector
from db_connectors.mysql_connector import MySQLConnector


def read_config(file_name: str) -> dict[str, int|dict]:
    config_file = open(file_name)
    config = json.load(config_file)
    config_file.close()

    return config
        

def wire_up(algorithm: str, db_type: str) -> AbstractAlgorithm:
    assert algorithm_name in ["datafly", "mondrian"]
    assert db_type in ["es", "mysql"]

    db_connector: AbstractAPI = EsConnector() if db_type == "es" else MySQLConnector()

    if algorithm == "datafly":
        return Datafly(db_connector)        

    if algorithm == "mondrian":
        return Mondrian(db_connector)
    
    raise Exception(f"No algorithm implementation name '{algorithm}' exists!")


if __name__ == '__main__':
    config_file_path = "configs/adults_config.json"
    
    # db_type = "es"
    db_type = "mysql"
    # algorithm_name = "datafly"
    algorithm_name = "mondrian"
    algorithm = wire_up(algorithm_name, db_type)

    config = read_config(config_file_path)

    start_time = time.time()

    print(f"Running {algorithm_name} with k={config['k']}...")

    algorithm.run(config)
    
    exec_time = float(time.time() - start_time)

    ncp = algorithm.calculate_ncp()
        
    print("NCP %0.2f" % ncp + "%")
    print("Run for %0.2f" % exec_time + " seconds")
