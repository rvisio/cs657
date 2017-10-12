#!/usr/bin/env python
from pyspark import SparkContext, SparkConf
import operator

conf = SparkConf().setMaster("local[*]").setAppName("spark_stripes")
sc = SparkContext(conf=conf)

def stripeMap(data):
    prevMovie = None
    out = []

    data = data.split(',')
    pairDict = {}
    pairList = []
    for firstMovie in xrange(0,len(data)):
        for secondMovie in xrange(firstMovie+1, len(data)):
            pair = (int(data[firstMovie]), int(data[secondMovie]))
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
pairs = output.flatMap(stripeMap)
out = pairs.map(lambda line: line).countByKey()

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
sorted_movies = sorted(out.items(), key=operator.itemgetter(1), reverse=True)
for i in range(0,20):
    print sorted_movies[i]
#out.coalesce(1).saveAsTextFile('xyzzz')
