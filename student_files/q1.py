import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,udf
from pyspark.sql.types import BooleanType
# you may add more import if you need to


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 1").getOrCreate()
# YOUR CODE GOES BELOW
@udf(returnType=BooleanType())
def is_valid(col):
    if col:
        return bool(eval(col))
    return False
# Read file
df = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .option("delimiter", ",")
    .option("quotes", '"')
    .csv("hdfs://%s:9000/assignment2/part1/input/" % (hdfs_nn))
)


df_filtered = df.filter(not ((col("Rating") >= 1.0) & (
    col("Rating").isNotNull()) & (is_valid(col("Reviews")))))

df_filtered.show()

# Write file
df_filtered.write.csv(
    "hdfs://%s:9000/assignment2/output/question1/" % (hdfs_nn), header=True)

