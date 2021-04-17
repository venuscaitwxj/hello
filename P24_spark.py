from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from functools import reduce
from pyspark.sql.types import *
import pandas as pd


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

# filtering
range1 = avg_df.filter(avg_df.range == 1)
range1Item = range1.select("movieid").rdd.flatMap(lambda x: x).collect()

range2 = avg_df.filter(avg_df.range == 2)
range2Item = range2.select("movieid").rdd.flatMap(lambda x: x).collect()

range3 = avg_df.filter(avg_df.range == 3)
range3Item = range3.select("movieid").rdd.flatMap(lambda x: x).collect()

range4 = avg_df.filter(avg_df.range == 4)
range4Item = range4.select("movieid").rdd.flatMap(lambda x: x).collect()

range5 = avg_df.filter(avg_df.range == 5)
range5Item = range5.select("movieid").rdd.flatMap(lambda x: x).collect()

#result dataframe

columns = ["range","movieid"]
Row1 = spark.createDataFrame([(1,range1Item)], columns)
Row2 = spark.createDataFrame([(2,range2Item)], columns)
Row3 = spark.createDataFrame([(3,range3Item)], columns)
Row4 = spark.createDataFrame([(4,range4Item)], columns)
Row5= spark.createDataFrame([(5,range5Item)], columns)

Rows = [Row1,Row2,Row3,Row4,Row5]
df_whole = reduce(DataFrame.unionAll, Rows)


#output
df_whole.show()
df_whole.repartition(1).write.csv("output")