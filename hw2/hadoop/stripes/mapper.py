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
            stripeDict = {}
            for pair in movieList:
                if movie != pair:
                    try:
                        stripeDict[pair] = stripeDict[pair]+1
                    except:
                        stripeDict[pair] = 1
            dictList = []
            for key, value in stripeDict.iteritems():
                temp = [key,value]
                dictList.append(temp)

            print '%s\t%s' % (movie, str(dictList))
    
        movieList = []
    curUser = userId 
    
    try:
        if float(rating) >= 4:
            movieList.append(movieId)
    except:
        #e = sys.exc_info()[0]
        #print e
        continue
