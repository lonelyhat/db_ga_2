import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,max,min
# you may add more import if you need to


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 2").getOrCreate()
# YOUR CODE GOES BELOW
df = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .option("delimiter", ",")
    .option("quotes", '"')
    .csv("hdfs://%s:9000/assignment2/part1/input/" % (hdfs_nn))
)

df_good = df.groupby(["Price Range","City"]).agg(max("Rating")).withColumn("Rating",col("max(Rating)"))
df_bad = df.groupby(["Price Range", "City"]).agg(
    min("Rating")).withColumn("Rating",col("min(Rating)"))
df_both = df_good.union(df_bad)
df_result = df_both.join(df,on=["Price Range","City","max(Rating"],how="inner")
df_result.show()
df_result.write.csv(
    "hdfs://%s:9000/assignment2/output/question1/" % (hdfs_nn), header=True)
