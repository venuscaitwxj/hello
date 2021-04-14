from pyspark import SparkConf, SparkContext

import string

pattern = sys.argv[1]

conf = SparkConf().setMaster('local').setAppName('DistributedGrep')
sc = SparkContext(conf = conf)

lines = sc.textFile("input.txt")
pythonLines = lines.filter(lambda line : pattern in line)
#print(pythonLines + "\n")
#lst = pythonLines.collect()


#for ele in lst:
#    print ele
pythonLines.saveAsTextF1ile("output.txt")