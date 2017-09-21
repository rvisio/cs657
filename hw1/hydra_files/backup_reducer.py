#!/usr/bin/env python

import sys
from math import sqrt

word2count = {}
year_count = {}
totalCount = 0
rangecount = {}
includeMinMax = False
for line in sys.stdin:
    line = line.strip()

    word,count, year = line.split()
    totalCount +=1
    try:
        count = int(count)
    except ValueError:
        continue
   
   # add to the year_count dictionary 
    try: 
        year_count[year] = year_count[year] + count
    except:
        year_count[year] = count

    # range count
    try:
        rangecount[word] = rangecount[word]+count
    except:
        rangecount[word] = count
   
   # add to the word count dict
    try:
        word2count[(word,year)] = word2count[(word, year)]+count
    except:
        word2count[(word,year)] = count

def getWindowCount(start_year):
    totalWindowCount = 0
    for x in range(1,4):
        try: 
            totalWindowCount = totalWindowCount + year_count[str(start_year+x)]
        except:
            continue

    return totalWindowCount

def getStDev(word, start_year):
    windowCount = getWindowCount(start_year)
    stdev = 0.0
    # get the total avg over 4 years 
    word_window_avg = word2count[(word,str(start_year+1))]/(windowCount * 1.0)
    # get avg for each year in the window, append to a list
    year_avg_list = []
    for x in range(1,4):
        year_avg = 0
        try:
            year_avg = word2count[(word,str(start_year+x))] / (year_count[str(start_year+x)] * 1.0)
        except:
            continue

        year_avg_list.append(year_avg)
    
    differences = [x - word_window_avg for val in year_avg_list]

    sq_diff = [d ** 2 for d in differences]

    ssd = sum(sq_diff)
    variance = ssd/ len(year_avg_list)
    sd = sqrt(variance)

    sq_diff = []
    differences = []

    #calculate stdev
    #stdev = numpy.std(year_avg_list, axis=0)
    return sd
        
year = 1984
while year <= 2017:
    windowCount = getWindowCount(year)
    for word in word2count.keys():
        # if the word is in our window print out values
        if int(word[1]) > year and int(word[1]) <= year+4:

            # calcuate average for window
            std =  getStDev(word[0], year)
            print 'window is %s:%s \t%s\t%s\t%s\t%s\tavg in window is  %.6f\t std in window is %.6f' % (str(year+1), str(year+4), str(word[0]), word2count[word], str(word[1]), year_count[word[1]],word2count[word]/(windowCount * 1.0), std)

            #print 'window is %s:%s \t%s\t%s\t%s\t%s\tavg i %.6f' % (str(year+1), str(year+4), str(word[0]), word2count[word], str(word[1]), year_count[word[1]],word2count[word]/(year_count[word[1]] * 1.0))

    if year != 2016:
        year = year + 4
    else:
        year = year + 1



if includeMinMax:
    wordmax = {}
    wordmin = {}
    for word in word2count.keys():
        try:
            if wordmax[word[0]] < word2count[word]:
                wordmax[word[0]] = word2count[word]
        except:
            wordmax[word[0]] = word2count[word]

        try:
            if wordmin[word[0]] > word2count[word]:
                wordmin[word[0]] = word2count[word]
        except:
            wordmin[word[0]] = word2count[word]

    for word in word2count.keys():
    #    print word
        print '%s\t%s\t%s\t%s\tavg is %.6f\t max value is %s\t min value is %s' % (str(word[0]), word2count[word], str(word[1]), year_count[word[1]],word2count[word]/(year_count[word[1]] * 1.0), str(wordmax[word[0]]), str(wordmin[word[0]]))
    #    print "ValueHistogram:" +  word2count[word]
    #    print '%s\t%s\t%d\tavg is %.6f' % (word, word2count[word], totalCount, word2count[word]/totalCount)
        #print word + ' ' + str(word2count[word]) + 'ValueHistogram:' + word 
        #print "ValueHistogram:" + "word" + "\t" + word2count[word[0]] 
