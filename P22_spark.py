from pyspark import SparkConf, SparkContext
import string
import sys
import re


conf = SparkConf().setMaster('local').setAppName('Url')
sc = SparkContext(conf = conf)


RDDvar = sc.textFile("access_log")

words = RDDvar.map(lambda line: line.split())

result = words.map(lambda word: (word[6],1))

aggreg1 = result.reduceByKey(lambda a, b: a+b)

output =aggreg1.collect()

for ele in output:
    print(ele)