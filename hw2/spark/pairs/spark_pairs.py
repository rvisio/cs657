import operator

from pyspark import SparkContext, SparkConf
import itertools


# Spark setup stuff
conf = SparkConf().setMaster("local[*]").setAppName("spark_pairs_test")
sc = SparkContext(conf=conf)

schema = 'userId movieId rating timestamp'

def combinePairs(data):
    data = data.split(',')

    return[v for v in itertools.combinations(data,2)]


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

for i in range(0,20):
    print sorted_movies[i]




#for key in out.keys():
#    print str(key) + ' ' + str(out[key])

