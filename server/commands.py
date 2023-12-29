from pymongo import MongoClient
import time

class MongoConnection:
    def __init__(self, uri: str, db_name: str) -> None:
        self.uri = uri
        self.db_name = db_name
        self.client = None
        self.db_connection = None

    def __enter__(self):
        self.client = MongoClient(self.uri)
        self.db_connection = self.client[self.db_name]
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.client is not None:
            self.client.close()
            self.db_connection = None

def fetch_details_by_title(words: list[str], limit: int = 20) -> list:
    start_time = time.time()

    with MongoConnection("mongodb://root:example@localhost:27017/", "Recommendations_project") as mongo_connection:
        if mongo_connection.db_connection is None:
            raise ValueError("Connection is not open. Call open_connection before querying.")
        
        collection = mongo_connection.db_connection.movies_by_cosine

        # result_list = []

        # for word in words:
            # Query to find documents matching the word in title_i or title_j
        query = {
            "$or": [
                {"title_i": {"$in": words[:2]}},
                {"title_j": {"$in": words[:2]}}
            ]
        }

        # Projection to include only necessary fields
        projection = {"_id": 0, "title_i": 1, "title_j": 1, "cosine_similarity": 1}

        # Sorting by cosine_similarity in descending order and limiting to top 20
        cursor = collection.find(query, projection).sort("cosine_similarity", -1).limit(10)

        # Convert cursor to list of dictionaries
        documents = list(cursor)

        #     # Append the list of documents for the current word to the result list
        #     result_list.append(documents)

        time_taken = time.time() - start_time
        print(f'Time taken for the query: {time_taken}')
        return documents

movies = ["Avatar", "The Avengers"]
result = fetch_details_by_title(movies)
for doc in result:
    print(doc)
