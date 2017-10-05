#!/usr/bin/env python

import sys

pairCount = {}
for line in sys.stdin:
    #coming in as word, pair, count

    first,second,count = line.split()

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
    print '%s\t%s\t%s' % (pair[0], pair[1], str(pairCount[(pair[0],pair[1])]))
