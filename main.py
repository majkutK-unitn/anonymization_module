from db_connectors.ec_connector import EsConnector


if __name__ == '__main__':
    connector = EsConnector()
    connector.run_query({"term": {"_id": {"value": "WS_m3IcBJ6zU8THO1VUA"}}})