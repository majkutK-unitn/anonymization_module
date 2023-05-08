import json
import time

from algorithms.mondrian.mondrian import Mondrian

from db_connectors.ec_connector import EsConnector


def wire_up():
    config_file = open('configs/adults_config.json')
    config = json.load(config_file)

    es_connector = EsConnector()
    mondrian = Mondrian(es_connector, config)

    config_file.close()

    start_time = time.time()

    mondrian.run()
    
    exec_time = float(time.time() - start_time)
    ncp = mondrian.calculate_ncp()

    print("K=", config["k"])
    print("NCP %0.2f" % ncp + "%")
    print("Run for %0.2f" % exec_time + " seconds")
    

if __name__ == '__main__':    
    wire_up()