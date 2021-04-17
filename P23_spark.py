from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, IntegerType, StringType
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("Basics").master('local').getOrCreate()

conf = SparkConf().setMaster('local').setAppName('DistributedGrep')
sc = SparkContext(conf = conf)

# read csv file
df = spark.read.csv("GOOGLE.csv").toDF("Date", "Open", "High", "Low","Close","Adj Close","Volume")

# make sure the columns are in right type
df = df.withColumn('Date', F.col('Date').cast(StringType()))
df = df.withColumn('Close', F.col('Close').cast(DoubleType()))

# with year
df_year = df.withColumn('Year',F.year(F.to_timestamp(df.Date, 'yyyy-MM-dd')))
result = df_year.groupby("Year").agg(F.mean("Close").alias('Avg_price'))

# output
print('Output has also been recorded into a csv file')
result.show()

result.repartition(1).write.csv("output")



