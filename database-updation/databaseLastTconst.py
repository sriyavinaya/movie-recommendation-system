from pymongo import MongoClient
import json

def get_last_tconst(collection_name):
    # Load MongoDB configuration from a JSON file
    with open('dbConfig.json') as config_file:
        config = json.load(config_file)
                
    mongo_connection_string = config.get('mongo_connection_string', '')
    mongo_database_name = config.get('mongo_database_name', '')

    # Connect to MongoDB
    client = MongoClient(mongo_connection_string)
    database = client[mongo_database_name]

    try:
        # Access the specified collection
        collection = database[collection_name]

        # Retrieve the last document, sorted by '_id' in descending order
        last_document = collection.find_one(sort=[('_id', -1)])

        if last_document:
            # Return the value of the 'tconst' field from the last document
            return last_document.get('tconst', None)
        else:
            print(f"No documents found in the collection: {collection_name}")
            return None

    except Exception as e:
        print(f"Error retrieving last document: {e}")
        return None

    finally:
        # Close the MongoDB connection
        client.close()

# Example usage:
last_tconst = get_last_tconst("movies")  # Replace "movies" with your actual collection name

if last_tconst:
    print(f"The last 'tconst' value is: {last_tconst}")
else:
    print("Failed to retrieve the last 'tconst' value.")
