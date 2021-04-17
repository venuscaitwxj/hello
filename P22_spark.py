from pyspark import SparkConf, SparkContext
import string
import sys
import re


conf = SparkConf().setMaster('local').setAppName('Url')
sc = SparkContext(conf = conf)

pattern = re.compile(r'\"(.*)\"') #define the pattern to search

RDDvar = sc.textFile("access_log")

# find all line with pattern
words = RDDvar.map(lambda line: re.findall(pattern,line))

# take the element
words = words.map(lambda lst: lst[0].split()[1] if len(lst[0].split()) >1 else None)

# to string (in case its unicode object)
result = words.filter(bool).map(str)

# mapping
result = words.map(lambda word: (word,1))

# reducing
aggreg1 = result.reduceByKey(lambda a, b: a+b)

# print
output =aggreg1.collect()

for ele in output:
    print(ele)