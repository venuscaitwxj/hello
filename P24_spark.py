from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *



spark = SparkSession.builder.appName("Movie").master('local').getOrCreate()



# read csv file
df = spark.read.csv("ratings.csv").toDF("userid", "movieid","rating", "timestamp")
df = df.withColumn('rating', F.col('rating').cast(FloatType()))

# aggregate range as average

avg_df = df.groupby("movieid").agg(F.mean("rating").alias('avg_rating'))


avg_df = avg_df.withColumn('range',F.when((F.col("avg_rating") >= 0) & (F.col("avg_rating") <= 1), 1)\
    .when((F.col("avg_rating") >1) & (F.col("avg_rating") <= 2), 2)\
    .when((F.col("avg_rating") >2) & (F.col("avg_rating") <= 3), 3)\
    .when((F.col("avg_rating") >3) & (F.col("avg_rating") <= 4), 4)\
    .when((F.col("avg_rating") >4) & (F.col("avg_rating") <= 5), 5))


## collect list
df2 =avg_df.groupby("range").agg(F.collect_list("movieid").alias("movieidnames"))



#output
df2.show()
#df2 = df2.withColumn('movieid', F.col('movieid').cast(StringType()))
df_whole.repartition(1).write.csv("output")