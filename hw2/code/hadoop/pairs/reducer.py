#!/usr/bin/env python

import sys, csv, operator

movieTitleDict = {}
def loadMovieNames():
    with open('ml-latest-small/movies.csv') as f:
#    with open('/home/rjarvis4/ml-latest-small/movies.csv') as f:
        for row in csv.reader(f):
            movieId = row[0]
            movieTitle = row[1]

            movieTitleDict[movieId] = movieTitle

pairCount = {}
for line in sys.stdin:
    #coming in as word, pair, count
    first,second,count = line.split()

    pair = (int(first),int(second))
    pair = sorted(pair)
    pair = tuple(pair)
    try:
        count = int(count)
    except:
        continue

    #try: 
        #pairCount[(second,first)] = pairCount[(second,first)]+count
    #except:
    try:
        pairCount[pair] = pairCount[pair]+count
        #print 'pair ' + str(pair) + ' is now at ' + str(pairCount[pair]) + ' just added ' + str(count)
    except:
        pairCount[pair] = count
        #print 'pair ' + str(pair) + ' is now at ' + str(pairCount[pair])

loadMovieNames()



sorted_movies = sorted(pairCount.items(), key=operator.itemgetter(1),reverse=True)
#for x in sorted_movies:
#    if x[1] %2 != 0:
#        print x
for i in range(0,20):
    print '%s\t%s\t%s' % (movieTitleDict[str(sorted_movies[i][0][0])],movieTitleDict[str(sorted_movies[i][0][1])], str(sorted_movies[i][1]))

# iterate through sorted_movies and print similar to the block below

#for pair in pairCount.keys():
#    print '%s\t%s\t%s' % (movieTitleDict[str(pair[0])], movieTitleDict[str(pair[1])], str(pairCount[(pair[0],pair[1])]))


