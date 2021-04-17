from pyspark import SparkConf, SparkContext

import string
import sys

pattern = sys.argv[1]

conf = SparkConf().setMaster('local').setAppName('DistributedGrep')
sc = SparkContext(conf = conf)

lines = sc.textFile("input.txt")
pythonLines = lines.filter(lambda x : pattern in x)
lst = pythonLines.collect()

for ele in lst:
    print ele
