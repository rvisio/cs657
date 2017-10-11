#!/usr/bin/env python 

import sys

curUser = 0
movieList = []
pairList = []

movieDictionary = {}
for line in sys.stdin:

    line = line.strip()
    userId,movieId,rating,timestamp=line.split(',')

    #if the user id has changed 
    """if curUser != userId and curUser != 0:
        #begin the pairs approach for mapping
        for movie in movieList:
            for pair in movieList:
                if movie != pair:
                    print '%s\t%s\t%s' % (movie,pair,'1')
        movieList = []
    curUser = userId 
    """
    try:
        if float(rating) >= 4:
            movieDictionary.setdefault(userId, []).append(movieId)

    except:
        #e = sys.exc_info()[0]
        #print e
        continue

for curUserId in movieDictionary.keys():
    movies = list(movieDictionary[curUserId])

    sort_movies = sorted(movies)

    for first in xrange(0,len(sort_movies)):
            for second in xrange(first+1, len(sort_movies)):
                if first != second:
                    print str(sort_movies[first]) + '\t' + str(sort_movies[second]) + '\t' + '1'

