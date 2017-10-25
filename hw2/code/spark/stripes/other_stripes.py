
import operator
from pyspark import SparkContext, SparkConf
import itertools
from collections import Counter
import csv
    
def make_a_stripe(data):
    my_dict = {}
    y = data.split(',')
    for mov1, mov2 in itertools.combinations(y,2):
            if mov1 not in my_dict.keys():
                my_dict[mov1] = {mov2 : 0}
            if mov2 not in my_dict[mov1]:
                my_dict[mov1][mov2] = 0
            my_dict[mov1][mov2] += 1
    return [(movie, stripe) for movie,stripe in my_dict.items()]

def sum_dicts(accum_dict,add_dict):
    for k,v in add_dict.items():
        if k in accum_dict:
            accum_dict[k] += v
        else:
            accum_dict[k] = v
    return accum_dict
    
def stripe_conditional(incoming_stripe):
    total_movie = sum(incoming_stripe[1].values())
    for k,v in incoming_stripe[1].items():
        incoming_stripe[1][k] = float(v)/float(total_movie)
    for k in list(incoming_stripe[1]):
        if incoming_stripe[1][k] < 0.8:
            del incoming_stripe[1][k]
    return incoming_stripe
    
def titles():
    movieDict={}
    with open('/Users/robjarvis/cs657/hw2/ml-latest-small/movies.csv') as f:
        for row in csv.reader(f):
            movieId = row[0]
            movieTitle = row[1]
            movieDict[movieId] = movieTitle
    return movieDict
        
conf = SparkConf().setMaster("local[*]").setAppName("spark_pairs_test")
sc = SparkContext(conf=conf)
raw_data = sc.textFile("/Users/robjarvis/cs657/hw2/ml-latest-small/ratings.csv")
clean_file = raw_data.map(lambda x: x.split(",")).filter(lambda x: float(x[2]) >= 4.0).map(lambda x: [x[0],x[1]])
movie_occurences = clean_file.reduceByKey(lambda x,y: x + "," + y).map(lambda movie: movie[1])
movie_list = movie_occurences.flatMap(make_a_stripe)
out = movie_list.reduceByKey(sum_dicts)

#results = out.flatMapValues(lambda x: [(k,v) for k,v in x.items()]).map(lambda x: ((x[0],x[1][0]),x[1][1]))
#out2 = results.collectAsMap()
#top_movies = sorted(out2.items(), key=operator.itemgetter(1), reverse=True)
#for i in range(0,20):
#    print movie_titles[top_movies[i][0][0]] + "," + movie_titles[top_movies[i][0][1]] + "," + str(top_movies[i][1])

probs = out.map(stripe_conditional) #L o L
filter_probs = probs.filter(lambda x: len(x[1]) >0)


probs_results = probs.flatMapValues(lambda x: [(k,v) for k,v in x.items()]).map(lambda x: ((x[0],x[1][0]),x[1][1]))
out_results = probs_results.collect()

movie_titles = titles()
for i in range(0,20):
    print movie_titles[out_results[i][0][0]] + "," + movie_titles[out_results[i][0][1]] + "," + str(out_results[i][1])





