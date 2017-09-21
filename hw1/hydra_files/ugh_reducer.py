#!/usr/bin/env python

import sys

word2count = {}
year_count = {}
year_word_dict = {}
totalCount = 0
for line in sys.stdin:
    line = line.strip()

    word,count, year = line.split()
    totalCount +=1
    try:
        count = int(count)
    except ValueError:
        continue

   # add to the year_count dictionary 

    '''    try: 
            year_count[year] = year_count[year] + count
        except:
            year_count[year] = count'''

    if int(year) >= 1985:
   # add to the word count dict
        try:
            word2count[(word,year)] = word2count[(word, year)]+count
        except:
            word2count[(word,year)] = count

loop = True
year = 1986
while loop == True:
    for word in word2count.keys():
        key_tuple = (word[0],str(year-1))

        if key_tuple in word2count.keys():
            print 'the year is %s\t word %s\t word count for that year is %s' % (str(year-1), str(word[0]),str(word2count[(word[0],str(year-1))]))


        #print '%s\t%s' % (str(year-1), year_count[str(year-1)])
        #try:
        #except:
#            print 'word %s\t didnt have a dict value for year %s' % (str(word[0]), str(year-1))


    year += 4
    if year+4 > 2018:
        loop = False


#for word in word2count.keys():
#    print '%s\t%s\t%s\t%s\tavg is %.6f' % (str(word[0]), word2count[word], str(word[1]), year_count[word[1]],word2count[word]/(year_count[word[1]] * 1.0))
#    print "ValueHistogram:" +  word2count[word]
#    print '%s\t%s\t%d\tavg is %.6f' % (word, word2count[word], totalCount, word2count[word]/totalCount)
    #print word + ' ' + str(word2count[word]) + 'ValueHistogram:' + word 
    #print "ValueHistogram:" + "word" + "\t" + word2count[word[0]] 
