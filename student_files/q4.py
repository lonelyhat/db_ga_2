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
df = (df.withColumn("Cuisine Style", regexp_replace("Cuisine Style", "\[", ""))
      .withColumn("Cuisine Style", regexp_replace("Cuisine Style", "\]", ""))
      .withColumn("Cuisine Style", split(col("Cuisine Style"),", "))
      .withColumn("Cuisine Style", explode("Cuisine Style"))
      .withColumn("Cuisine Style", regexp_replace("Cuisine Style", "'", ""))
      .withColumn("Cuisine Style", trim(col("Cuisine Style")))
)
result = (df.select("City","Cuisine Style")
    .groupBy("City", "Cuisine Style").agg(count("*").alias("Count"))
    .select(col("City").alias("City"),col("Cuisine Style").alias("Cuisine"),col("Count").alias("Count"),)
    .sort(col("City").asc(), col("Cuisine").desc(),col("Count").desc())
)

result.show()

result.write.csv(
    "hdfs://%s:9000/assignment2/output/question4/" % (hdfs_nn), header=True
)