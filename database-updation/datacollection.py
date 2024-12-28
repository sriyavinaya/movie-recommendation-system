from pymongo import MongoClient
import json

def get_unique_values(database_url, database_name, collection_name):
    client = MongoClient(database_url)
    db = client[database_name]
    collection = db[collection_name]

    # Get unique values for languages, genres, streaming services, and ratings
    languages = list(set(filter(None, collection.distinct("Language"))))
    
    # Split and flatten the genres list, then create a set to ensure uniqueness
    genres = list(set(genre.strip() for movie in collection.find({}, {"Genre": 1}) for genre in movie.get("Genre", "").split(',')))

    streaming_services = []
    for movie in collection.find({}, {"StreamingService.StreamingService": 1}):
        streaming_services.extend([service["StreamingService"] for service in movie.get("StreamingService", [])])

    ratings = list(set(filter(None, collection.distinct("Rated"))))

    # Remove duplicates
    languages = list(set(languages))
    streaming_services = list(set(streaming_services))
    ratings = list(set(ratings))

    client.close()

    return languages, genres, streaming_services, ratings

# Replace the following with your MongoDB connection details
with open('dbConfig.json') as config_file:
        config = json.load(config_file)
                
database_url = config.get('mongo_connection_string', '')
database_name = config.get('mongo_database_name', '')
collection_name = config.get('mongo_collection_name', '')

# Call the function to get unique values
languages, genres, streaming_services, ratings = get_unique_values(database_url, database_name, collection_name)

# Print the results
print("Unique Genres:", genres)
print("Unique Streaming Services:", streaming_services)
print("Unique Ratings:", ratings)