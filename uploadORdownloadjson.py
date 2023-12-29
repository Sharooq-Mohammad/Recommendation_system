# from pymongo import MongoClient
# import json

# # Replace the following placeholders with your MongoDB Atlas connection string and details
# connection_string = "mongodb+srv://NoSQL:1234@atlascluster.evpliqi.mongodb.net"
# database_name = "sample_mflix"
# collection_name = "movies"
# output_file_path = "movies.json"

# # Connect to MongoDB Atlas
# client = MongoClient(connection_string)
# db = client[database_name]
# collection = db[collection_name]

# # Query the entire collection
# cursor = collection.find()

# # Convert the cursor to a list of dictionaries
# data = list(cursor)

# # Close the MongoDB connection
# client.close()

# # Save the data to a JSON file
# with open(output_file_path, 'w') as json_file:
#     json.dump(data, json_file, default=str)

# print(f"Data from {collection_name} collection saved to {output_file_path}")

# from pyspark.sql import SparkSession
# from pyspark import SparkContext

# sc = SparkContext(master='local', appName='ETL_Job')
# spark = SparkSession(sparkContext=sc)

# movies_df = spark.read.format('json').option('inferSchema', 'true').load(r'source_files/movies.json')

# database_name = "movies_db"
# collection_name = "movies"

# # Write the PySpark DataFrame to MongoDB
# movies_df.write.format("com.mongodb.spark.sql.DefaultSource")\
#     .option("uri", "mongodb+srv://NoSQL:1234@atlascluster.evpliqi.mongodb.net") \
#     .option("database", database_name) \
#     .option("collection", collection_name) \
#     .mode("overwrite") \
#     .save()

import json, time
from pymongo import MongoClient

start = time.time()

try:
    client = MongoClient('mongodb://root:example@localhost:27017/')

    db = client['Recommendations_project']
    # collection = db['source_movies']
    collection = db['movies_by_cosine']

    # with open('dataset/movies.json') as file:
    #     data = json.load(file)

    # with open('../test/movies_by_cosine.json', "r") as file:
    #     data = [json.loads(line) for line in file]

    # print('started uploading')

    # collection.insert_many(data)
    # collection.replace_many({}, data, upsert=True)    

    results = collection.find({"title_i":"Avatar"})
    results_list = list(results)
    print(results_list[:10])

    # print(collection.count_documents({}))
    # result = collection.delete_many({})

    print(time.time() - start)

except Exception as e:
    print(f"Error connecting to MongoDB: {e}")
finally:
    client.close()

# print(type(data[0]))
