import asyncio
import gzip
import aiohttp
import pandas as pd
from pymongo import MongoClient
import json
from aiohttp import ClientSession
from io import BytesIO
import sys

async def fetch_watch_providers(session, movie_id, tmdb_api_key):
    watch_providers_url = f"https://api.themoviedb.org/3/movie/{movie_id}/watch/providers?api_key={tmdb_api_key}"

    try:
        async with session.get(watch_providers_url) as response:
            response.raise_for_status()

            if response.status == 200:
                watch_providers_data = await response.json()
                results = watch_providers_data.get("results", {})

                # Extract streaming providers for India (IN)
                india_providers = results.get("IN", {})
                streaming_providers = india_providers.get("flatrate", [])

                # Format the data
                formatted_providers = []
                if not streaming_providers:
                    formatted_provider = {
                        "StreamingService": "None Found",
                        "LogoPath": "",
                    }
                    formatted_providers.append(formatted_provider)
                else:
                    for provider in streaming_providers:
                        formatted_provider = {
                            "StreamingService": provider["provider_name"],
                            "LogoPath": f"https://image.tmdb.org/t/p/w500{provider['logo_path']}",
                        }
                        formatted_providers.append(formatted_provider)

                return formatted_providers

            else:
                print(f"Failed to fetch watch providers for movie {movie_id}.")

    except aiohttp.ClientError as e:
        print(f"Error fetching watch providers for movie {movie_id}: {e}")

    return [{"StreamingService": "None Found", "LogoPath": ""}]

async def fetch_tmdb_data(session, semaphore, row, tmdb_api_key):
    async with semaphore:
        tconst = row.get('tconst', '')
        title_type = row.get('titleType', 'movie')  # Set default value to 'movie' if not present

        movie_document = {
            "tconst": tconst,
            "titleType": title_type,
        }

        if not tconst:
            print("Error: 'tconst' key not found in the row dictionary.")
            return movie_document

        tmdb_id_url = f"https://api.themoviedb.org/3/find/{tconst}?api_key={tmdb_api_key}&external_source=imdb_id"

        try:
            async with session.get(tmdb_id_url, timeout=15) as response:
                response.raise_for_status()
                tmdb_id_data = await response.json()
                tmdb_id_results = tmdb_id_data.get("movie_results", [])

                if tmdb_id_results:
                    tmdb_id = tmdb_id_results[0].get("id", None)

                    if tmdb_id:
                        keywords_url = f"https://api.themoviedb.org/3/movie/{tmdb_id}/keywords?api_key={tmdb_api_key}"
                        poster_url = f"https://api.themoviedb.org/3/movie/{tmdb_id}?api_key={tmdb_api_key}"
                        reviews_url = f"https://api.themoviedb.org/3/movie/{tmdb_id}/reviews?api_key={tmdb_api_key}"

                        async with session.get(keywords_url) as keywords_response, session.get(poster_url) as poster_response, session.get(reviews_url) as reviews_response:
                            keywords_data = await keywords_response.json()
                            poster_data = await poster_response.json()
                            reviews_data = await reviews_response.json()

                            await asyncio.sleep(1 / 45)

                            if keywords_response.status == 200 and poster_response.status == 200 and reviews_response.status == 200:
                                # Fetching watch providers data
                                watch_providers_data = await fetch_watch_providers(session, tmdb_id, tmdb_api_key)

                                movie_document.update({
                                    "Title": row['originalTitle'],
                                    "Keywords": [keyword['name'] for keyword in keywords_data.get("keywords", [])],
                                    "PosterAlt": f"https://image.tmdb.org/t/p/w500{poster_data.get('poster_path', '')}" if poster_data.get('poster_path') else "",
                                    "StreamingService": watch_providers_data,
                                    "Reviews": reviews_data.get("results", []),
                                })
                            else:
                                print(f"Failed to fetch Keywords, Poster, or Reviews data for {row['tconst']}.")

                    else:
                        print(f"TMDB ID not found for {tconst}.")

        except aiohttp.ClientError as e:
            print(f"Error fetching TMDB ID for {tconst}: {e}")

        return movie_document

async def fetch_omdb_data(session, row, omdb_api_key):
    omdb_url = f"http://www.omdbapi.com/?i={row['tconst']}&apikey={omdb_api_key}"

    try:
        async with session.get(omdb_url) as omdb_response:
            omdb_response.raise_for_status()

            if omdb_response.status == 200:
                omdb_data = await omdb_response.json()
                return {
                    "Year": omdb_data.get("Year", ""),
                    "Rated": omdb_data.get("Rated", ""),
                    "Released": omdb_data.get("Released", ""),
                    "Runtime": omdb_data.get("Runtime", ""),
                    "Genre": omdb_data.get("Genre", ""),
                    "Director": omdb_data.get("Director", ""),
                    "Writer": omdb_data.get("Writer", ""),
                    "Actors": omdb_data.get("Actors", ""),
                    "Plot": omdb_data.get("Plot", ""),
                    "Language": omdb_data.get("Language", ""),
                    "Country": omdb_data.get("Country", ""),
                    "Poster": omdb_data.get("Poster", ""),
                    "RottenTomatoesRating": omdb_data["Ratings"][1]["Value"] if len(omdb_data.get("Ratings", [])) > 1 else "",
                    "IMDBRating": omdb_data.get("imdbRating", ""),
                }

            else:
                print(f"Failed to fetch OMDB data for {row['tconst']}.")

    except aiohttp.ClientError as e:
        print(f"Error fetching OMDB data for {row['tconst']}: {e}")

    return None

async def process_single_movie(session, tconst, tmdb_api_key, omdb_api_key):
    async with ClientSession() as session:
        semaphore = asyncio.Semaphore(1)  # Adjust the semaphore limit to 1 for processing one movie at a time

        # Fetch TMDB and OMDB data for the specified IMDb movie ID
        tmdb_data = await fetch_tmdb_data(session, semaphore, {'tconst': tconst}, tmdb_api_key)
        omdb_data = await fetch_omdb_data(session, {'tconst': tconst}, omdb_api_key)

        if tmdb_data and omdb_data:
            # Combine TMDB and OMDB data
            tmdb_data.update(omdb_data)

            # Fetch watch providers data
            watch_providers_data = await fetch_watch_providers(session, tmdb_data['id'], tmdb_api_key)
            tmdb_data["StreamingService"] = watch_providers_data

            # Insert the combined data into MongoDB
            client = MongoClient(mongo_connection_string)
            db = client[mongo_database_name]
            collection = db[mongo_collection_name]

            collection.insert_one(tmdb_data)
            print(f"Document for {tconst} inserted into MongoDB successfully.")

            client.close()

async def main():
    print("Started Process")

    with open('dbConfig.json') as config_file:
        config = json.load(config_file)

    global mongo_connection_string, mongo_database_name, mongo_collection_name
    mongo_connection_string = config.get('mongo_connection_string', '')
    mongo_database_name = config.get('mongo_database_name', '')
    mongo_collection_name = config.get('mongo_collection_name', '')
    tmdb_api_key = config.get('tmdb_api_key', '')
    omdb_api_key = config.get('omdb_api_key', '')

    if not (mongo_connection_string and tmdb_api_key and omdb_api_key):
        print("MongoDB connection string or API keys not found in config.json.")
        exit()

    if len(sys.argv) != 2:
        print("Usage: python script_name.py <IMDb_movie_ID>")
        exit()

    tconst = sys.argv[1]

    await process_single_movie(None, tconst, tmdb_api_key, omdb_api_key)

if __name__ == "__main__":
    asyncio.run(main())
