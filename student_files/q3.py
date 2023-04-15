import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col, arrays_zip, regexp_replace, trim

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
df = df.select("ID_TA", "Reviews")

# 新加两个review, date，用], [这个形式分开
# 这两个\\是防止[和]被认成regex的一部分
df = df.withColumn("review", split(df["Reviews"], "\\], \\[").getItem(0))\
        .withColumn("date", split(df["Reviews"], "\\], \\[").getItem(1))

# 通过', '这个东西来分两条评论
# [['a, b', 'c' 变成 [[['a, b, c']，里面第一条是[['a, b第二条是c'
df = df.withColumn("review", split(col("review"), "', '"))\
        .withColumn("date", split(col("date"), "', '"))

# 把同一ID 的不同评论分开
new_df = df.withColumn("review_date", explode(arrays_zip("review", "date")))\
        .select("ID_TA",
                col("review_date.review").alias("review"),
                col("review_date.date").alias("date")
                )

# 去掉单引号
new_df = new_df.withColumn("review", regexp_replace(new_df.review, "'", ""))\
        .withColumn("date", regexp_replace(new_df.date, "'", ""))

# 去掉[ 去掉]
# 这两个\\是防止[和]被认成regex的一部分
new_df = new_df.withColumn("review", regexp_replace(new_df.review, "\\[", ""))\
        .withColumn("date", regexp_replace(new_df.date, "\\]", ""))

# trim一下，不然有的靠左有的靠右
new_df = new_df.withColumn("review", trim(new_df.review))\
        .withColumn("date", trim(new_df.date))
new_df.show()

new_df.write.csv(
    "hdfs://%s:9000/assignment2/output/question3/" % (hdfs_nn),
    header=True,
    emptyValue="",
)