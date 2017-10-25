from pyspark import SparkContext, SparkConf
import itertools,csv,operator

def loadMovieNames():
    movieDict={}
    with open('/Users/robjarvis/cs657/hw2/ml-latest-small/movies.csv') as f:
        for row in csv.reader(f):
            movieId = row[0]
            movieTitle = row[1]

            movieDict[movieId] = movieTitle
    return movieDict


def stripes(row):
    row = row.split(',')

    stripeDict = {}
    for movieA, movieB in itertools.combinations(row,2):

        try:
            stripeDict[movieA][movieB] = stripeDict[movieA][movieB]+1
        except:
            try:
                stripeDict[movieA][movieB] = 1
            except:
                stripeDict[movieA] = {movieB:1}

    return[(movie, stripe) for movie,stripe in stripeDict.items()]


def mergeStripes(accumulator, stripe):
    for key,value in stripe.iteritems():
        try:
            accumulator[key] = accumulator[key] + value
        except:

            accumulator[key] = value

    return accumulator
            


# Spark config stuff
conf = SparkConf().setMaster("local[*]").setAppName("spark_stripes")
sc = SparkContext(conf=conf)

# Read lines from the input file
lines = sc.textFile('/Users/robjarvis/cs657/hw2/ml-latest-small/ratings.csv')

#for each line strip and split on the comma
cleanLines = lines.map(lambda l: l.strip().split(','))

# remove all ratings that are under 4.0
removeLowRatings = cleanLines.filter(lambda l: float(l[2])>=4.0)

# lsit of highly rated movies aggregated by user
moviesByUser = removeLowRatings.map(lambda x: [x[0],x[1]]).reduceByKey(lambda user,movie: user + ',' + movie).map(lambda x:x[1])

# convert to stripes/dicts
movieStripes = moviesByUser.flatMap(stripes)

# merge counts together
mergedStripeSums = movieStripes.reduceByKey(mergeStripes)

#getVals = mergedStripeSums.collect()

processRdd = mergedStripeSums.flatMapValues(lambda mergedDict: [(key,value) for key,value in mergedDict.iteritems()]).map(lambda x: ((x[0], x[1][0]),x[1][1]))
processRdd.coalesce(1).saveAsTextFile('xyzzz')
getVals = processRdd.collectAsMap()

movieDict = loadMovieNames()
sorted_movies = sorted(getVals.items(), key=operator.itemgetter(1), reverse=True)

for i in range(0,20):
    print str(movieDict[sorted_movies[i][0][0]]) + ',' + str(movieDict[sorted_movies[i][0][1]])+ ' ' + str(sorted_movies[i][1])
#mergedStripeSums.coalesce(1).saveAsTextFile('xyzzz')
