from db_connectors.ec_connector import EsConnector


if __name__ == '__main__':
    connector = EsConnector()
    connector.search({"term": {"_id": {"value": "CTdo4IcBJ6zU8THOrfFz"}}})
    connector.get_partition_count()