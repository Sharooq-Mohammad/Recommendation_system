from typing import List
from pymongo import MongoClient
import os
import logging
import re

class MongoConnection:
    def __init__(self) -> None:
        self.connection_url = os.getenv("MONGO_URI")
        if not self.connection_url:
            raise Exception("Cannot connect to mongodb")
        
        self.database = os.getenv("MONGO_DATABASE", "Recommendations_project")
        self.page_size = os.getenv("MONGO_PAGE_SIZE", 12)

        self.__connect(self.connection_url)

    def __connect(self, uri: str):
        self.connection = MongoClient(self.connection_url)
        if not self.connection:
            raise Exception("Cannot connect to mongodb")
        self.__ping()
        
    def __ping(self):            
        try:
            self.connection.admin.command("ping")
            logging.info("Connection successfully established")
        except AttributeError:
            logging.error("ping failed. connection not established")
            raise Exception("Connection not established")
        except Exception:
            logging.error("ping failed")
            raise Exception("Cannot connect to mongodb")
        
    def find_movies_by_input(self, query_str: str, page: int= 0):
        db = self.connection[self.database]
        collection = db.get_collection("source_movies")

        regex_pattern = re.compile(query_str, re.IGNORECASE)

        fields_to_include = ["id", "title", "poster", "tagline"]
        projection = { field : 1 for field in fields_to_include}

        query = {
            "$or": [
                {"title": {"$regex": regex_pattern}}
                # {"cast": {"$regex": regex_pattern}},
                # {"genres": {"$regex": regex_pattern}},
                # {"plot": {"$regex": regex_pattern}},
                # {"directors": {"$regex": regex_pattern}},
            ]
        }
        offset = page * self.page_size

        logging.info("searching movies for input %s", query_str)
        cursor = collection.find(query, projection).skip(offset).limit(self.page_size + 1)
        result = list(cursor)
        logging.info("movies found for input %s : %s", query_str, len(result))
        print(f"movies found for input {query_str}: {len(result)}")

        next_page = page + 1 if len(result) > self.page_size else -1
        return result[:self.page_size], next_page
    
    def get_staged_movies(self, movies: List[str]):
        db = self.connection[self.database]
        collection = db.get_collection("source_movies")

        query = { "title" : { "$in": movies }}

        fields_to_include = ["id", "title", "poster", "release_date"]
        projection = { field : 1 for field in fields_to_include}

        cursor = collection.find(query, projection)
        documents = list(cursor)

        print(f"No. of movies found: {len(documents)}")
        return documents
    
    def get_recommended_movies(self, movies: List[str]):
        db = self.connection[self.database]
        movies_by_cosine = db.get_collection("movies_by_cosine")
        source_movies = db.get_collection("source_movies")

        query = {
            "$or": [
                { "title_i" : { "$in": movies }},
                { "title_j" : { "$in": movies }}
            ]
        }
        order = {"cosine_similarity": -1}
        # projection = { field : 1 for field in ("title_i", "title_j", "cosine_similarity") }

        cursor = movies_by_cosine.find(query).sort(order)
        movie_cosine_map = {}
        movies = set(movies)

        for doc in cursor:
            if doc["title_i"] in movies and doc["title_j"] in movies:
                continue
            
            movie_title = ''
            if doc["title_i"] in movies:
                movie_title = doc["title_j"] 
            else:
                movie_title = doc["title_i"]
            print(movie_title, doc["cosine_similarity"])
            movie_cosine_map[movie_title] = doc["cosine_similarity"]

            if len(movie_cosine_map) == self.page_size:
                break

        if cursor:
            cursor.close()

        recommended_movies = list(movie_cosine_map.keys())

        query = { "title" : { "$in": recommended_movies }}
        fields_to_include = ["id", "title", "poster", "release_date"]
        projection = { field : 1 for field in fields_to_include}

        cursor = source_movies.find(query, projection)
        documents = list(cursor)
        documents.sort(key=lambda x: movie_cosine_map[x["title"]], reverse=True)

        print(f"No. of movies found: {len(documents)}")
        return documents