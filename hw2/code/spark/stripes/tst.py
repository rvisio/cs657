#!/usr/bin/env python
from pyspark import SparkContext, SparkConf
import itertools,csv,operator

conf = SparkConf().setMaster("local[*]").setAppName("spark_stripes")
sc = SparkContext(conf=conf)

def loadMovieNames():
    movieDict={}
    with open('/Users/robjarvis/cs657/hw2/ml-latest-small/movies.csv') as f:
        for row in csv.reader(f):
            movieId = row[0]
            movieTitle = row[1]

            movieDict[movieId] = movieTitle
    return movieDict

def stripeMap(data):
    prevMovie = None
    out = []

    data = data.split(',')
    pairDict = {}
    pairList = []
    for firstMovie,secondMovie in itertools.combinations(data, 2):
#    for firstMovie in xrange(0,len(data)):
#        for secondMovie in xrange(firstMovie+1, len(data)):
#        pair = (int(data[firstMovie]), int(data[secondMovie]))
        pair = (int(firstMovie), int(secondMovie))
        pair = sorted(pair)
        pair = tuple(pair)

        pairList.append(pair)

    for pairKey in pairList:
        try: 
            pairDict[pairKey] = pairDict[pairKey]
        except:
            pairDict[pairKey] = 1

    listToReturn = []
    for key in pairDict.keys():
        movie1 = key[0]
        movie2 = key[1]
        outputList = []
        outputList.append("%s,%s" % (str(movie1), str(movie2)))
        listToReturn.append(outputList)
    return listToReturn 


"""    if data:
        moviePairs = {}
        data = data.split(',')
        if data:
            print data
            for firstMovie in xrange(0,len(data)):
                if firstMovie:
                    for secondMovie in xrange(firstMovie+1, len(data)):
                        if secondMovie:
                            pair = (int(firstMovie), int(secondMovie))
                            pair = sorted(pair)
                            pair = tuple(pair)
                            #print pair"""

def reduceByDict(theDict, somethingElse):
    for key in somethingElse.keys():
        try:
            coDict[key] = coDict[key]+1
        except:
            coDict[key] = 1

    return coDict

#    for movie in data:
#        print movie

# Read lines from the input file
lines = sc.textFile('/Users/robjarvis/cs657/hw2/ml-latest-small/ratings.csv')

#for each line strip and split on the comma
cleanLines = lines.map(lambda l: l.strip().split(','))

# remove all ratings that are under 4.0
removeLowRatings = cleanLines.filter(lambda l: float(l[2])>=4.0)

# Mapper
"""
#TODO
need to map user id and a dictionary of movies and values that are counted for
"""

output = removeLowRatings.map(lambda x: [x[0],x[1]]).reduceByKey(lambda user,movie: user + ',' + movie).map(lambda x:x[1])

#reduceByUserId = output.reduceByKey(lambda user, movie: user + ',' + movie)
#reduceByUserId = output.reduceByKey(lambda user, movie: user + ',' + movie)
#movies = reduceByUserId.map(lambda movie: movie[1])
def combinations(row):
    row = row.split(',')

    return [(v) for v in itertools.combinations(row,2)]
def stripeCombinations(row):
    l = row[1]
    k = row[0]
#    return [(k,v) for v in itertools.combinations(l,2)]
    return [(k,v) for v in l]

stuff = output.flatMap(combinations).groupByKey().map(lambda x: (x[0],list(x[1])))
test = stuff.flatMap(stripeCombinations)
newStuff = test.map(lambda line: [line]).countByKey()

#newStuff.coalesce(1).saveAsTextFile('xyzzz')

#pairs = output.flatMap(stripeMap)
#pairs = output.flatMap(lambda x: [v for v in itertools.combinations(x,2)])
#out = pairs.map(lambda line: line).countByKey()

#stripeStuff = pairs.map(lambda line: str(line))
#newOutput = pairs.map(lambda line: [line, '1']).countByKey()


#groupByUserId = output.reduceByKey

#reduceByUserId = output.map(returnUserDict)
#output = removeLowRatings.map(lambda x: [x[0], x[1]]).reduceByKey(lambda user, movie: list((user,movie))).map(lambda movie: movie[1])

#Reducer
# Reduce these counts and output 

"""finalDict = {}
for key in newOutput.keys():
    try:
        finalDict[key] = finalDict[key]+1
    except:
        finalDict[key] = 1
for key in finalDict.keys():
    if finalDict[key] > 1:
        print key
print type(finalDict)"""
movieDict = loadMovieNames()
sorted_movies = sorted(newStuff.items(), key=operator.itemgetter(1), reverse=True)
for i in range(0,20):
    print str(movieDict[sorted_movies[i][0][0]]) + ',' + str(movieDict[sorted_movies[i][0][1]])+ str(sorted_movies[i][1])
#pairs.coalesce(1).saveAsTextFile('xyzzz')
