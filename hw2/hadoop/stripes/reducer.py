#!/usr/bin/env python

import sys,ast

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
        try:
            stripeDict[(stripeMatch, movie)] = stripeDict[(stripeMatch, movie)]+sum
        except:
            try:
                stripeDict[(movie,stripeMatch)] = stripeDict[(movie,stripeMatch)]+sum
            except:
                stripeDict[(movie,stripeMatch)] = sum

# need to modify so that only the top 20 frequent pairs are produced
for stripe in stripeDict.keys():
    try:
        print '%s\t%s\t%s' % (stripe[0], stripe[1], str(stripeDict[(stripe[0], stripe[1])]))
    except:
        print sys.exc_info()[0]
"""
    try:
        count = int(count)
    except:
        continue

    try: 
        pairCount[(second,first)] = pairCount[(second,first)]+count
    except:
        try:
            pairCount[(first,second)] = pairCount[(first,second)]+count
        except:
            pairCount[(first,second)] = count

for pair in pairCount.keys():
    print '%s\t%s\t%s' % (pair[0], pair[1], str(pairCount[(pair[0],pair[1])]))"""
