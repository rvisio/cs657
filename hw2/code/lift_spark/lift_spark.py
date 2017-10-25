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
    counter = 0
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

def calculateLift(data):
    totalStripeMovie = sum(data[1].values())

    stripeLength = len(data[1].values())

    for key,value in data[1].items():
       data[1][key] = (float(value)/float(totalStripeMovie)) / (float(stripeLength)/users)
       if data[1][key] < 1.6:
           del data[1][key]

    return data


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
moviesByUser = removeLowRatings.map(lambda x: [x[0],x[1]])

listMoviesByUser = moviesByUser.reduceByKey(lambda user,movie: user + ',' + movie).map(lambda x:x[1])

users = moviesByUser.map(lambda x: x[0]).distinct().count()

# convert to stripes/dicts
movieStripes = listMoviesByUser.flatMap(stripes)

#movieStripes.coalesce(1).saveAsTextFile('movieStripes')



# merge counts together
mergedStripeSums = movieStripes.reduceByKey(mergeStripes)
#mergedStripeSums.coalesce(1).saveAsTextFile('mergedStripes')

conditionalProbability = mergedStripeSums.map(calculateLift)

removeEmpty = conditionalProbability.filter(lambda line: len(line[1])>0)


mapValues = removeEmpty.flatMapValues(lambda x: [(k,v) for k,v in x.items()]).map(lambda x: ((x[0],x[1][0]),x[1][1]))
movieDict = loadMovieNames()
addTitles = mapValues.map(lambda line: [movieDict[line[0][0]], movieDict[line[0][1]], line[1]])

addTitles.coalesce(1).saveAsTextFile('condProbOutputs')
#processRdd = conditionalProbability.flatMapValues(lambda mergedDict: [(key,value) for key,value in mergedDict.iteritems()]).map(lambda x: ((x[0], x[1][0]),x[1][1]))
#getVals = processRdd.collectAsMap()

#processRdd.coalesce(1).saveAsTextFile('xyz')
