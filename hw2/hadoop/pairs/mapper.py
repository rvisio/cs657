#!/usr/bin/env python 

import sys

curUser = 0
movieList = []
pairList = []
for line in sys.stdin:

    userId,movieId,rating,timestamp=line.split(',')

    #if the user id has changed 
    if curUser != userId and curUser != 0:
        #begin the pairs approach for mapping
        for movie in movieList:
            for pair in movieList:
                tuplePair = (movie,pair)
                reverseTuplePair = (pair,movie)
                if movie != pair and tuplePair not in pairList and reverseTuplePair not in pairList:
                    pairList.append((movie,pair))
                    print '%s\t%s\t%s' % (movie,pair,'1')
        movieList = []
        pairList = []
    curUser = userId 
    
    try:
        if float(rating) >= 4:
            movieList.append(movieId)
    except:
        #e = sys.exc_info()[0]
        #print e
        continue
