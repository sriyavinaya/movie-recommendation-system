from pymongo import MongoClient
import json

def delete_last_n_items(database_url, database_name, collection_name, n=100):
    client = MongoClient(database_url)
    db = client[database_name]
    collection = db[collection_name]

    last_n_items = collection.find().sort("_id", -1).limit(0)

    item_ids = [item["_id"] for item in last_n_items]

    result = collection.delete_many({"_id": {"$in": item_ids}})

    print(f"Deleted {result.deleted_count} items.")

    client.close()

with open('dbConfig.json') as config_file:
        config = json.load(config_file)

database_url = config.get('mongo_connection_string', '')
database_name = config.get('mongo_database_name', '')
collection_name = config.get('mongo_collection_name', '')

delete_last_n_items(database_url, database_name, collection_name, n=100)
