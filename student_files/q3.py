import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, explode

# you may add more import if you need to


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 3").getOrCreate()
# YOUR CODE GOES BELOW
json_schema = "array<struct<ReviewContent:string, Date:string>>"
df = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .option("delimiter", ",")
    .option("quotes", '"')
    .csv("hdfs://%s:9000/assignment2/part1/input/" % (hdfs_nn))
)
df = df.select(["ID_TA", "Reviews"])
df_json = df.withColumn("Reviews", from_json("Reviews", json_schema))
df_flattened = df_json.select("ID_TA", explode("Reviews").alias("Reviews"))
df_new = df_flattened.select("ID_TA", "Reviews.ReviewContent as Review", "Reviews.Date as Date")
df_new.show()
df_new.write.csv(
    "hdfs://%s:9000/assignment2/output/question3/" % (hdfs_nn),
    header=True,
    emptyValue="",
)