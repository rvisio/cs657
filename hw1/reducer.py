#!/usr/bin/env python

import sys

word2count = {}
totalCount = 0
for line in sys.stdin:
    line = line.strip()

    word,count = line.split('\t',1)
    totalCount +=1
    try:
        count = int(count)
    except ValueError:
        continue

    try:
        word2count[word] = word2count[word]+count
    except:
        word2count[word] = count

for word in word2count.keys():
#    print '%s\t%s\t%d\tavg is %.6f' % (word, word2count[word], totalCount, word2count[word]/totalCount)
    #print word + ' ' + str(word2count[word]) + 'ValueHistogram:' + word 
    print "ValueHistogram:" + "word" + "\t" + word2count[word] 
 
