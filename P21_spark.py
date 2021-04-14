from pyspark import SparkConf, SparkContext

import string

conf = SparkConf().setMaster('local').setAppName('DistributedGrep')
sc = SparkContext(conf = conf)

lines = sc.textFile("input.txt")
lines.persist() # Spark keeps lines in RAM
lines.count()
pythonLines = lines.filter(lambda line : 'Web' in line)
lst = pythonlines.collect()
for ele in lst:
    print ele
