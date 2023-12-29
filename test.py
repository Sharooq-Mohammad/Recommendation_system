from pyspark.sql import SparkSession
from pyspark import SparkContext

sc = SparkContext(appName='test')
spark = SparkSession(sparkContext=sc)

# movies_df = spark.read.format('json').option('inferSchema', 'true').load(r'/test-files/dataset/movies.json')
df = spark.createDataFrame([{'name':'John','age':30},{'name':'rock','age':40},{'name':'Jack','age':31}])

# print(df.count())
df = df.coalesce(1)

df.write.json('/test-files/dataset/test', mode='overwrite')
