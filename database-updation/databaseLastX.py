from pymongo import MongoClient
import json

with open('dbConfig.json') as config_file:
        config = json.load(config_file)
                
mongo_connection_string = config.get('mongo_connection_string', '')
mongo_database_name = config.get('mongo_database_name', '')

client = MongoClient(mongo_connection_string)

database = client[mongo_database_name]

collection = database["movies"]  # Replace "movies" with your actual collection name

last_10_entries = collection.find().sort([('_id', -1)]).limit(1)

# Print the results
for entry in last_10_entries:
    print(entry)