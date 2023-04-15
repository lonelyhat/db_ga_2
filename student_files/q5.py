import sys 
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StringType, StructField, StructType
# you may add more import if you need to
from pyspark.sql.functions import from_json, col, explode, array, array_sort, count

# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 5").getOrCreate()
json_schema = ArrayType(StructType([StructField("name", StringType(), False)]))

# YOUR CODE GOES BELOW
df = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .option("delimiter", ",")
    .option("quotes", '"')
    .parquet("hdfs://%s:9000/assignment2/part2/input/" % (hdfs_nn))
)
df=df.drop(col("crew"))
df_expanded = df.withColumn("actor_indv",explode(from_json(col("cast"), json_schema).getField("name"))).drop(col("cast"))
df_expanded1=df_expanded.alias("df_expanded1")
df_expanded2=df_expanded.alias("df_expanded2")
df_joined=df_expanded1.join(df_expanded2,on=["movie_id"],how="inner").where(col("df_expanded1.actor_indv")<col("df_expanded2.actor_indv"))
df_joined.show()
df_result=df_joined.groupby(['df_expanded1.actor_indv',"df_expanded2.actor_indv"]).agg(count("movie_id")).where(col('count(movie_id)')>=2)
df_result.show()
df_result_with_movie = df_result.join(df_joined,on=['df_expanded1.actor_indv',"df_expanded2.actor_indv"],how='inner').select(df_joined['movie_id'],df_joined['title'],df_result['actor_indv'].alias('actor1'),df_result['actor_indv'].alias('actor2'))
df_result_with_movie.show()