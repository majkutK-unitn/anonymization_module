import json

import time

from os import getenv

from algorithms.datafly.datafly import Datafly
from algorithms.mondrian.mondrian import Mondrian

from interfaces.abstract_algorithm import AbstractAlgorithm
from interfaces.abstract_api import AbstractAPI

from db_connectors.es_connector import EsConnector
from db_connectors.mysql_connector import MySQLConnector

import argparse

parser = argparse.ArgumentParser('Anonymization Module')
parser.add_argument('--algorithm', type=str, default='mondrian',
                    help="K-Anonymity algorithm: mondrian / datafly (default: mondrian)")
parser.add_argument('--backend', type=str, default='es',
                    help="Backend to use: es / mysql (default: es)")
parser.add_argument('--config', type=str, default='adults_config.json',
                    help="Name of the config file: str (default: adults_config.json)")


def read_config(file_name: str) -> dict[str, int|dict]:
    config_file = open(file_name)
    config = json.load(config_file)
    config_file.close()

    return config
        

def wire_up(algorithm_name: str, db_type: str) -> AbstractAlgorithm:
    assert algorithm_name in ["Datafly", "Mondrian"]
    assert db_type in ["Elasticsearch", "MySQL"]

    db_connector: AbstractAPI = EsConnector() if db_type == "Elasticsearch" else MySQLConnector()

    if algorithm_name == "Datafly":
        return Datafly(db_connector)        

    if algorithm_name == "Mondrian":
        return Mondrian(db_connector)        


def main(args: dict):
    config_file_path = f"configs/{args.config}"
    db_backend = "Elasticsearch" if args.backend == "es" else "MySQL"
    algorithm_name = "Mondrian" if args.algorithm == "mondrian" else "Datafly"

    algorithm = wire_up(algorithm_name, db_backend)

    config = read_config(config_file_path)

    start_time = time.time()

    print(f"""Running anonymization
    - target dataset: {getenv('MYSQL_TABLE_NAME') if db_backend == "MySQL" else getenv('INDEX_NAME')}
    - database: {db_backend}
    - config file: {config_file_path}
    - algorithm: {algorithm_name}
    - k: {config['k']}""")

    algorithm.run(config)
    
    exec_time = float(time.time() - start_time)

    ncp = algorithm.calculate_ncp()
        
    print("NCP %0.2f" % ncp + "%")
    print("Run for %0.2f" % exec_time + " seconds")


if __name__ == '__main__':
    args = parser.parse_args()
    main(args)
