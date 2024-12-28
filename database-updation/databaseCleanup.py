from pymongo import MongoClient
import json

with open('dbConfig.json') as config_file:
    config = json.load(config_file)

mongo_connection_string = config.get('mongo_connection_string', '')
mongo_database_name = config.get('mongo_database_name', '')

client = MongoClient(mongo_connection_string)

database = client[mongo_database_name]

collection = database["movies"]

delete_criteria = {"Year": ""}

result = collection.delete_many(delete_criteria)

print(f"Deleted {result.deleted_count} documents.")
