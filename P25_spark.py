from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, IntegerType, StringType,FloatType
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("Astro").master('local').getOrCreate()


# read csv file
df = spark.read.csv("Meteorite_Landings.csv").toDF("name", "col1", "col2", "type","mass","col6","col7","col8","col9","col10")

# make sure the columns are in right type
df = df.withColumn('type', F.col('type').cast(StringType()))
df = df.withColumn('mass', F.col('mass').cast(FloatType()))

# averaging
result = df.groupby("type").agg(F.mean("mass").alias('avg_mass'))


# output to screen

result.show()

# and save to output
result.repartition(1).write.csv("output")
