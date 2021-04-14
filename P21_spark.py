from pyspark import SparkConf, SparkContext

import string

conf = SparkConf().setMaster('local').setAppName('DistributedGrep')
sc = SparkContext(conf = conf)

lines = sc.textFile("input.txt")
pythonLines = lines.filter(lambda line : 'Web' in line)
print (pythonLines + "\n")
#lst = pythonLines.collect()


#for ele in lst:
#    print ele
