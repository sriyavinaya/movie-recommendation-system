from pymongo import MongoClient
import json

with open('dbConfig.json') as config_file:
    config = json.load(config_file)

def count_movies_by_tconst(mongo_connection_string, database_name, collection_name, batch_size=30000):
    client = MongoClient(mongo_connection_string)
    db = client[database_name]
    collection = db[collection_name]

    # Get the total count of documents
    total_count = collection.count_documents({})

    # Calculate the number of batches needed
    num_batches = (total_count // batch_size) + (1 if total_count % batch_size != 0 else 0)

    # Process in batches
    for batch_num in range(num_batches):
        skip_count = batch_num * batch_size

        # Aggregate to count occurrences of each unique tconst for the current batch
        pipeline = [
            {"$skip": skip_count},
            {"$limit": batch_size},
            {"$group": {"_id": "$tconst", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ]

        result = collection.aggregate(pipeline)

        # Print the results for the current batch
        print(f"Movies Count by tconst - Batch {batch_num + 1}:")
        for entry in result:
            print(f"tconst: {entry['_id']}, Count: {entry['count']}")

    client.close()

if __name__ == "__main__":                
    mongo_connection_string = config.get('mongo_connection_string', '')
    database_name = config.get('mongo_database_name', '')
    collection_name = config.get('mongo_collection_name', '')

    count_movies_by_tconst(mongo_connection_string, database_name, collection_name, batch_size=30000)
