import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, regexp_replace, explode, count, col,trim
import ast
# you may add more import if you need to


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 4").getOrCreate()
# YOUR CODE GOES BELOW
df = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .option("delimiter", ",")
    .option("quotes", '"')
    .csv("hdfs://%s:9000/assignment2/part1/input/" % (hdfs_nn))
)

df = df.withColumn("Cuisine Style", col("Cuisine Style").strip(']['))
df = df.withColumn("Cuisine Style", col("Cuisine Style").split(", "))
df = df.withColumn("Cuisine Style", col("Cuisine Style").strip('\''))
result = df.select("City","Cuisine Style")
result = result.groupBy("City", "Cuisine Style").agg(count("*").alias("Count")).sort(col("City").asc(), col("Cuisine").desc(),col("Count").desc())
result = result.select(col("City").alias("City"),col("Cuisine Style").alias("Cuisine"),col("Count").alias("Count"),)

result.show()

result.write.csv(
    "hdfs://%s:9000/assignment2/output/question4/" % (hdfs_nn), header=True
)