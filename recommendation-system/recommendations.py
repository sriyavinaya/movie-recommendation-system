import math
import pandas as pd
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from pymongo import MongoClient
import json

# Function to prepare data
def prepare_data(x):
    return str.lower(x.replace(" ", ""))

# Function to create soup
def create_soup(x):
    return x['Genre'] + ' ' + ', '.join(x['Keywords']) + ' ' + x['Actors'] + ' ' + x['Director'] + ' ' + str(x['IMDBRating']) + ' ' + x['Rated']

# Function to get recommendations
def get_recommendations(title, cosine_sim):
    global result
    title = title.lower() 
    if title in indices:
        idx = indices[title]
        sim_scores = list(enumerate(cosine_sim[idx]))
        sim_scores = sorted(sim_scores, key=lambda x: x[1], reverse=True)
        sim_scores = sim_scores[1:11]  # Return top 10 recommendations
        movie_indices = [i[0] for i in sim_scores]
        result = netflix_data.iloc[movie_indices]
        result.reset_index(inplace=True)
        return result
    else:
        print(f"Movie '{title}' not found in the database.")
        return pd.DataFrame()


# Read MongoDB connection string, database name, and collection name from config.json
with open('reccConfig.json') as config_file:
    config_data = json.load(config_file)
    mongo_connection_string = config_data.get('mongo_connection_string', '')
    mongo_database_name = config_data.get('mongo_database_name', '')
    mongo_collection_name = config_data.get('mongo_collection_name', '')

# Connect to MongoDB
print("Connecting to MongoDB...")
try:
    client = MongoClient(mongo_connection_string)
    db = client[mongo_database_name]
    collection = db[mongo_collection_name]
    print("Connected to MongoDB successfully!")
except Exception as e:
    print("Error connecting to MongoDB:", e)
    exit(1)

# Fetch data from MongoDB
print("Fetching data from MongoDB...")
cursor = collection.find({})
netflix_data = pd.DataFrame(list(cursor))

# Data processing
print("Processing data...")
netflix_data['Genre'] = netflix_data['Genre'].astype('str').apply(prepare_data)

# Check and handle NaN values in 'Keywords' column
netflix_data['Keywords'] = netflix_data['Keywords'].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else '')
netflix_data['Actors'] = netflix_data['Actors'].astype('str').apply(prepare_data)
netflix_data['Director'] = netflix_data['Director'].astype('str').apply(prepare_data)
netflix_data['IMDBRating'] = netflix_data['IMDBRating'].astype('str')
netflix_data['Rated'] = netflix_data['Rated'].astype('str')
netflix_data['soup'] = netflix_data.apply(create_soup, axis=1)

# Create CountVectorizer and cosine similarity matrix
print("Creating CountVectorizer and cosine similarity matrix...")
count = CountVectorizer(stop_words='english')
count_matrix = count.fit_transform(netflix_data['soup'])
cosine_sim = cosine_similarity(count_matrix, count_matrix)
netflix_data.reset_index(inplace=True)
indices = pd.Series(netflix_data.index, index=netflix_data['Title'])

# User input for movie titles
print("Taking user input...")
user_movies = [input(f"Enter Movie {i+1} Title: ") for i in range(5)]

# Get recommendations based on user input
print("Getting recommendations...")
recommendations = pd.DataFrame()
for movie_title in user_movies:
    recommendations = pd.concat([get_recommendations(movie_title, cosine_sim), recommendations], ignore_index=True)

# Drop duplicate movies and sort by IMDb Score
print("Sorting and displaying top 10 recommendations...")
recommendations.drop_duplicates(subset=['Title'], keep='first', inplace=True)
recommendations.sort_values(by='IMDBRating', ascending=False, inplace=True)

# Display top 10 recommendations
top_recommendations = recommendations.head(10)
print("\nTop 10 Recommendations:")
print(top_recommendations[['Title', 'Genre', 'IMDBRating']])
