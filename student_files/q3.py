import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# you may add more import if you need to


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 3").getOrCreate()
# YOUR CODE GOES BELOW
df = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .option("delimiter", ",")
    .option("quotes", '"')
    .csv("hdfs://%s:9000/assignment2/part1/input/" % (hdfs_nn))
)
df = df.select(["ID_TA", "Reviews"])

# Define a UDF to extract the review content from the Reviews column
extract_review = udf(lambda x: x[0][0] if x and x[0] else None, StringType())

# Define a UDF to extract the review date from the Reviews column
extract_date = udf(lambda x: x[1][0] if x and x[1] else None, StringType())

# Apply the UDFs to create new columns for Review and Date
new_df = df.select("ID_TA", extract_review("Reviews").alias("Review"), extract_date("Reviews").alias("Date"))

new_df.show()

new_df.write.csv(
    "hdfs://%s:9000/assignment2/output/question3/" % (hdfs_nn),
    header=True,
    emptyValue="",
)