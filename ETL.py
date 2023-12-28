import time
start_time = time.time()

from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType, DoubleType, DateType
from pyspark.sql.functions import col, udf, from_json
from pyspark.ml.feature import SQLTransformer
from pyspark.ml.feature import CountVectorizer
from pyspark.sql import functions as F

import pandas as pd
import re

sc = SparkContext(master='local', appName='ETL_Job')
spark = SparkSession(sparkContext=sc)

# spark = SparkSession.builder \
#     .appName('ETL_Job') \
#     .config("spark.some.config.option", "config-value") \
#     .config("spark.driver.memory", "4g") \
#     .config("spark.executor.memory", "4g") \
#     .getOrCreate()

movies_df = spark.read.format('json').option('inferSchema', 'true').load(r'source_files/movies.json')
credits_df = spark.read.csv(r"source_files/credits.csv", header=True, multiLine=True, escape='"', inferSchema=True)

credits_df = credits_df.withColumnRenamed("movie_id", "id")

movies_df.createOrReplaceTempView('movies_df')
credits_df.createOrReplaceTempView('credits_df')

new_movies_df = spark.sql(f'select m.*, c.id credits_id, c.title credits_title, c.cast, c.crew from movies_df m join credits_df c on (m.id = c.id)')

selected_columns = ["id", "title", "cast", "crew", "keywords", "genres"]

df = new_movies_df.select(selected_columns)

movie_cast_schema = ArrayType(
    StructType([
        StructField("cast_id", IntegerType()),
        StructField("character", StringType()),
        StructField("credit_id", StringType()),
        StructField("gender", IntegerType()),
        StructField("id", IntegerType()),
        StructField("name", StringType()),
        StructField("order", IntegerType())
    ])
)

movie_crew_schema = ArrayType(
    StructType([
        StructField("credit_id", StringType()),
        StructField("department", StringType()),
        StructField("gender", IntegerType()),
        StructField("id", IntegerType()),
        StructField("job", StringType()),
        StructField("name", StringType())
    ])
)

movie_keywords_schema = ArrayType(
    StructType([
        StructField("id", IntegerType()),
        StructField("name", StringType())
    ])
)

movie_genres_schema = ArrayType(
    StructType([
        StructField("id", IntegerType()),
        StructField("name", StringType())
    ])
)

df = df.withColumn("cast", from_json(col("cast"), movie_cast_schema)) \
    .withColumn("crew", from_json(col("crew"), movie_crew_schema)) \
    .withColumn("keywords", from_json(col("keywords"), movie_keywords_schema)) \
    .withColumn("genres", from_json(col("genres"), movie_genres_schema))

@udf(ArrayType(StringType()))
def get_director(x):
    for i in x:
        if i["job"] == "Director":
            return [i["name"]]
    return None

df = df.withColumn("director", get_director("crew"))

@udf(ArrayType(StringType()))
def get_list(x):
    if isinstance(x, list):
        names = [i["name"] for i in x]
        names = [name for name in names if name]  # Remove None values
        if len(names) > 3:
            names = names[:3]
        return names
    return []

featured_columns = ["cast", "keywords", "genres"]

for column in featured_columns:
    df = df.withColumn(column, get_list(col(column)))

@udf(ArrayType(StringType()))
def clean_data(row):
    if isinstance(row, list):
        return [i.replace(" ", "").lower() for i in row]
    elif isinstance(row, str):
        return [row.replace(" ", "").lower()]
    return []

featured_columns = ['cast', 'keywords', 'director', 'genres']

for column in featured_columns:
    df = df.withColumn(column, clean_data(col(column)))

@udf(ArrayType(StringType()))
def merge_cols(keywords, cast, director, genres):
    return list(keywords + cast + director + genres)

df = df.withColumn('tokens', merge_cols("keywords", "cast", "director", "genres"))

df_2 = df.select('title', 'tokens')

# Use CountVectorizer to convert text data to a feature vector
cv = CountVectorizer(inputCol="tokens", outputCol="features")
cv_model = cv.fit(df_2)
count_matrix = cv_model.transform(df_2)

# Calculate cosine similarity
dot_udf = F.udf(lambda x, y: float(x.dot(y)), 'double')
cosine_sim = count_matrix.alias("i").join(count_matrix.alias("j"), F.col("i.title") < F.col("j.title")) \
    .select(
        F.col("i.title").alias("title_i"),
        F.col("j.title").alias("title_j"),
        dot_udf("i.features", "j.features").alias("cosine_similarity")
    )
# print(cosine_sim.count())

# cosine_sim.write.json('target_files', mode='overwrite')

# Reset index
df_2 = df_2.withColumn("index", F.monotonically_increasing_id())

# Create index Series
indices = df_2.select("title", "index").withColumnRenamed("index", "movie_index")

# Function to get recommendations
def get_recommendations(title, cosine_sim, indices):
    # idx = indices.filter(indices.title == title).select("movie_index").collect()[0][0]

    # Create a temporary view for cosine_sim DataFrame
    cosine_sim.createOrReplaceTempView("cosine_sim_view")

    # Use Spark SQL to filter and select relevant columns
    query = f"""
        SELECT title_i, title_j, cosine_similarity
        FROM cosine_sim_view
        WHERE title_i = "{title}" OR title_j = "{title}"
    """ # order by cosine_similarity desc
    
    similarity_scores = spark.sql(query).rdd.flatMap(
        lambda row: [(row["title_i"], row["cosine_similarity"]), (row["title_j"], row["cosine_similarity"])]
    ).collect()

    # similarity_scores = list(set(similarity_scores))
    similarity_scores = list(filter(lambda pair: pair[0] != title, similarity_scores))
    similarity_scores = sorted(similarity_scores, key=lambda x: x[1], reverse=True)
    similarity_scores = similarity_scores[:20]

    # # Extract movie indices
    # movies_indices = [ind[0] for ind in similarity_scores]

    # # Get movie titles
    # recommended_movies = indices.filter(indices.title.isin(movies_indices)).select("title")

    # return recommended_movies
    return spark.createDataFrame(similarity_scores, ['title', 'cosine_similarity'])

# Print recommendations
movie = "Superman"
print("################ Content Based System #############")
print(f"Recommendations for {movie}:")
get_recommendations(movie, cosine_sim, indices).show(truncate=False)

print(time.time() - start_time)

# df_2.printSchema()

# df_2.select('title').limit(20).show(truncate=False)

# database_name = "movies"
# collection_name = "movies_by_cosine"

# # Write the PySpark DataFrame to MongoDB
# cosine_sim.write.format("com.mongodb.spark.sql.DefaultSource")\
#     .option("uri", "mongodb+srv://NoSQL:1234@atlascluster.evpliqi.mongodb.net") \
#     .option("database", database_name) \
#     .option("collection", collection_name) \
#     .mode("overwrite") \
#     .save()

# pdf = cosine_sim.toPandas()
# pdf.to_json('new.json', orient='records')