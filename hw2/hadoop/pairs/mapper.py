#!/usr/bin/env python 

import sys

curUser = 0
movieList = []
for line in sys.stdin:

    userId,movieId,rating,timestamp=line.split(',')

    #if the user id has changed 
    if curUser != userId and curUser != 0:
        #begin the pairs approach for mapping
        for movie in movieList:
            for pair in movieList:
                if movie != pair:
                    print '%s\t%s\t%s' % (movie,pair,'1')
        movieList = []
    curUser = userId 
    
    try:
        if float(rating) >= 4:
            movieList.append(movieId)
    except:
        #e = sys.exc_info()[0]
        #print e
        continue
