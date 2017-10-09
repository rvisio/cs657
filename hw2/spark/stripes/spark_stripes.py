#!/usr/bin/env python
from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("local[*]").setAppName("spark_stripes")
sc = SparkContext(conf=conf)


# Read lines from the input file
lines = sc.textFile('/Users/robjarvis/cs657/hw2/ml-latest-small/ratings.csv')

#for each line strip and split on the comma
cleanLines = lines.map(lambda l: l.strip().split(','))

# remove all ratings that are under 4.0
removeLowRatings = cleanLines.filter(lambda l: float(l[2])>=4.0)

# Mapper

output = removeLowRatings.map(lambda x: [x[0], x[1]]).reduceByKey(lambda user, movie: list((user,movie))).map(lambda movie: movie[1])

output.coalesce(1).saveAsTextFile('count_output_small')
