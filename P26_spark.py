from pyspark import SparkConf, SparkContext

import string
import sys

pattern = "5.0"

#conf = SparkConf().setMaster('local[1]').setAppName('DistributedGrep')
conf = SparkConf().setAppName('DistributedGrep')
sc = SparkContext(conf = conf)

lines = sc.textFile("ratings.csv")
pythonLines = lines.filter(lambda x: pattern in x)
lst = pythonLines.collect()

for ele in lst:
    print ele
