#!/usr/bin/env python

import sys,ast
movieTitleDict = {}

def loadMovieNames():
    with open('movies.csv') as f:
        for row in csv.reader(f):
            movieId = row[0]
            movieTitle = row[1]

            movieTitleDict[movieId] = movieTitle

stripeDict = {}
for line in sys.stdin:
    #coming in as a dict with key[movie] and nested dicts within the dict
    # {'2': {'3': 1, '4': 1}}
    # the dict that is coming in is for a single user (assignment asks for movies that receive a High ranking from the same user)
    movie, pairList = line.split('\t')
    lists = []
    lists = ast.literal_eval(pairList)

    stripeDict[movie] = {}


    for stripeMatch,sum in lists:
        try:
            sum = int(sum)
        except:
            continue
        pair = (int(stripeMatch, int(movie)))

        pair = sort(pair)

        try:
            stripeDict[pair] = stripeDict[pair]+sum
        except:
            stripeDict[pair] = sum
            #try:
            #    stripeDict[(movie,stripeMatch)] = stripeDict[(movie,stripeMatch)]+sum
            #except:
            #    stripeDict[(movie,stripeMatch)] = sum

loadMovieNames()
# need to modify so that only the top 20 frequent pairs are produced

sorted_movies = sorted(pairCount.items(), key=operator.itemgetter(1),reverse=true)

for stripe in stripeDict.keys():
    try:
        print '%s\t%s\t%s' % (movieTitleDict[stripe[0]], movieTitleDict[stripe[1]], str(stripeDict[(stripe[0], stripe[1])]))
