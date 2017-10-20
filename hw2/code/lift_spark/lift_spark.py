#!/usr/bin/env python
from pyspark import SparkContext, SparkConf
import operator
import itertools
from collections import Counter
import csv


conf = SparkConf().setMaster("local[*]").setAppName("spark_stripes")
sc = SparkContext(conf=conf)


# Read lines from the input file
lines = sc.textFile('/Users/robjarvis/cs657/hw2/ml-latest-small/ratings.csv')

#for each line strip and split on the comma
cleanLines = lines.map(lambda l: l.strip().split(','))

# remove all ratings that are under 4.0
removeLowRatings = cleanLines.filter(lambda l: float(l[2])>=4.0)


output = removeLowRatings.map(lambda x: [x[0],x[1]]).reduceByKey(lambda user,movie: user + ',' + movie).map(lambda x:x[1])

def loadMovieNames():
    movieDict={}
    with open('/Users/robjarvis/cs657/hw2/ml-latest-small/movies.csv') as f:
        for row in csv.reader(f):
            movieId = row[0]
            movieTitle = row[1]

            movieDict[movieId] = movieTitle
    return movieDict

def combinations(row):
    row = row.split(',')

    return [(v) for v in itertools.combinations(row,2)]
def stripeCombinations(row):
    l = row[1]
    k = row[0]

    return [(k,v) for v in l]

stuff = output.flatMap(combinations).groupByKey().map(lambda x: (x[0],list(x[1])))
test = stuff.flatMap(stripeCombinations)
test.cache()
#stuff = test.map(lambda line: [line,'1']).countByKey()
#newOutput = test.map(Counter).reduce(lambda x,y: x+y)
newOutput = test.flatMap(lambda x: x)
tstThis = newOutput.map(lambda line: [line,'1']).countByKey()
#tstThis.coalesce(1).saveAsTextFile('xyzzz')

# tstThis contains a count of movieId and number of times it apepars in the pairs
#print tstThis['260']
totalMov = sum(tstThis.values())
movieTitleDict = loadMovieNames()
def removeLowCounts(row):
    moviePair = row[0]
   
    # calculating for movie b given movie a
    lift = 0.0

    movieACount = int(row[1])
    movieBCount = int(row[2])

    totalMovieCount = float(row[3])

    totalPairCount = movieACount + movieBCount

    # calculate the probability of this movie pair
#    movieAProb = float(totalPairCount)/float(row[3])
    movieAProb = float(movieACount)/totalMovieCount
    movieBProb = float(movieBCount)/totalMovieCount

    pairProb = float(totalPairCount)/totalMovieCount

    # Calculate P(A|B) 
    condProb = float(totalPairCount) / movieBProb  

    
    if condProp > 0.8:
        return [str(movieTitleDict[str(moviePair[0])]), str(movieTitleDict[str(moviePair[1])]), str(condProp)]

pairsAndCounts = test.map(lambda movie: [movie, tstThis[str(movie[0])], tstThis[str(movie[1])], totalMov])
filterOnLowCP = pairsAndCounts.map(removeLowCounts).filter(lambda x: x is not None)
#removeLow = filterOnLowCp 
pairsAndCounts.coalesce(1).saveAsTextFile('xyz')
#filterOnLowCP.coalesce(1).saveAsTextFile('xyz')
#sorted_movies = sorted(tstThis.items(), key=operator.itemgetter(1), reverse=True)
#for i in range(0,20):
#    print pairsAndCounts[i]
