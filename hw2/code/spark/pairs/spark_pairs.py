import operator

from pyspark import SparkContext, SparkConf
import itertools,csv


# Spark setup stuff
conf = SparkConf().setMaster("local[*]").setAppName("spark_pairs_test")
sc = SparkContext(conf=conf)

schema = 'userId movieId rating timestamp'

def combinePairs(data):
    data = data.split(',')

    return[v for v in itertools.combinations(data,2)]

def loadMovieNames():
    movieDict={}
    with open('/Users/robjarvis/cs657/hw2/ml-latest-small/movies.csv') as f:
        for row in csv.reader(f):
            movieId = row[0]
            movieTitle = row[1]

            movieDict[movieId] = movieTitle
    return movieDict

#read in the dataset to lines rdd
lines = sc.textFile('/Users/robjarvis/cs657/hw2/ml-latest-small/ratings.csv')
#lines = sc.textFile('/Users/robjarvis/cs657/hw2/ratings_small.csv')

#for each line strip and split on the comma 
cleanLines = lines.map(lambda l: l.strip().split(','))

# remove all ratings that are under 4.0
removeLowRatings = cleanLines.filter(lambda l: float(l[2])>=4.0)

# Mapper
# use reduce by key
output = removeLowRatings.map(lambda x: [x[0], x[1]])
reduceByUserId = output.reduceByKey(lambda user, movie: user + ',' + movie)

# iterate over movies, select pairs and emit count 1
movies = reduceByUserId.map(lambda movie: movie[1])

pairs = movies.flatMap(combinePairs)

out = pairs.map(lambda line: [line, '1']).countByKey()


# Reduce and count movie pair appearances that exist in pairs
#out.coalesce(1).saveAsTextFile('count_output_small')

#TODO
# load movie titles



sorted_movies = sorted(out.items(), key=operator.itemgetter(1), reverse=True)

movieTitleDict = loadMovieNames()

for i in range(0,20):
    print str(movieTitleDict[sorted_movies[i][0][0]]) + '\t' + str(movieTitleDict[sorted_movies[i][0][1]]) + '\t' + str(sorted_movies[i][1])




#for key in out.keys():
#    print str(key) + ' ' + str(out[key])

