from pymongo import MongoClient
import re
import time

class MongoConnection:
    def __init__(self, uri: str, db_name: str) -> None:
        self.uri = uri
        self.db_name = db_name
        self.client = None
        self.db_connection = None

    def open_connection(self):
        self.client = MongoClient(self.uri)
        self.db_connection = self.client[self.db_name]

    def close_connection(self):
        if self.client is not None:
            self.client.close()
            self.db_connection = None

# By default, open a connection when the module is imported
mongo_connection = MongoConnection("mongodb+srv://NoSQL:1234@atlascluster.evpliqi.mongodb.net/", "movies")
mongo_connection.open_connection()

def fetch_details_by_title(words: list[str], limit: int = 20) -> list:
    if mongo_connection.db_connection is None:
        raise ValueError("Connection is not open. Call open_connection before querying.")

    start_time = time.time()
    collection = mongo_connection.db_connection.movies_by_cosine
    
    fields_to_include = ["id", 'imdb', "plot", "genres", "title", 'type', "poster", "released", 'cast', 'directors', 'runtime']

    # Search for the word in title, cast, genres, and plot
    query = {
        "$or": [
            {"title_i": {"$in": words}},
            {"title_j": {"$in": words}}
        ]
    }

    # Sort the results by "cosine_similarity" in descending order and limit to top 20
    projection = {"_id": 0}  # Exclude the "_id" field from the results
    sort_order = [("cosine_similarity", -1)]  # Sort in descending order based on "cosine_similarity"
    result = collection.find(query, projection).sort(sort_order).limit(limit)
    
    time_taken = time.time() - start_time
    print(f'Time taken for the query: {time_taken}')
    result_list = list(result)
    return result_list

# Function to explicitly close the connection
def close_mongo_connection():
    mongo_connection.close_connection()

movies = ["Road to Ninja: Naruto the Movie", "Avatar", "The Avengers"]
result = fetch_details_by_title(movies)
close_mongo_connection()
print(len(result))