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

# get the word count for a word within a window
def getWindowWordCount(word, start_year):
    totalWordCount = 0
    for x in range(1,5):
        try:
        #print 'word2 ' + str(word2count[(word,str(start_year+x))])
            totalWordCount = totalWordCount + word2count[(word,str(start_year+x))]
        except:
            pass

    #print totalWordCount
    return totalWordCount

# Returns the total word count for a window
def getWindowCount(start_year):
    totalWindowCount = 0
    for x in range(1,5):
        try: 
            totalWindowCount = totalWindowCount + year_count[str(start_year+x)]
        except:
            pass

    return totalWindowCount

def getStDev(word, start_year):
    # get total word count for the window start year 
    stdev = 0.0
    # get the total avg over 4 years 
    window_word_count = getWindowWordCount(word,start_year)
    word_window_avg = window_word_count/(4.0)
        
    # get avg for each year in the window, append to a list
    year_avg_list = []
    for x in range(1,5):
        year_avg = 0
        try:

            year_avg = word2count[(word,str(start_year+x))]
        except:
            year_avg = 0

        year_avg_list.append(year_avg)
    

    mean_x = sum(year_avg_list) / len (year_avg_list)

    y = list(map(lambda z: (z - mean_x)**2, year_avg_list ))
    stdev = (sum(y)/(len(y)-1))**0.5 

    return stdev
    
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
        print '%s\t%s\t%s\t%s\tavg is %.6f\t max value is %s\t min value is %s' % (str(word[0]), word2count[word], str(word[1]), year_count[word[1]],word2count[word]/(year_count[word[1]] * 1.0), str(wordmax[word[0]]), str(wordmin[word[0]]))

else:
    year = 1984
    while year <= 2017:
        windowCount = getWindowCount(year)

        for word in word2count.keys():
            # if the word is in our window print out values
            if int(word[1]) > year and int(word[1]) <= year+4:
                #print word[1]

                window_word_count = getWindowWordCount(word[0], year)
                avg = window_word_count/4.0
                # calcuate average for window
                std =  getStDev(word[0], year)
                print 'window is %s:%s\tthe word is %s\tthe word appearss %s\ttimes in the year %s\t%s\tavg in window is  %.6f\t std in window is %s' % (str(year+1), str(year+4), str(word[0]), word2count[word], str(word[1]), year_count[word[1]],avg, str(std))

                if int(word[1]) == year +4:
                    nextYear = int(word[1]) + 1
                    try:
                        next_year_count = word2count[(word[0], str(nextYear))]
                        threshold = avg + (std * 2.0)
                        if next_year_count >= threshold:
                            print 'ALERT THERE IS A BREACH OF THRESHOLD %s\t%s\t%s' % (word[0], next_year_count, str(nextYear))

                    except:
                        pass




               # print 'window is %s:%s\t%.6f\tstd dev %s' % (str(year+1), str(year+4), word2count[word]/(windowCount*1.0), str(std))

                #print 'window is %s:%s \t%s\t%s\t%s\t%s\tavg i %.6f' % (str(year+1), str(year+4), str(word[0]), word2count[word], str(word[1]), year_count[word[1]],word2count[word]/(year_count[word[1]] * 1.0))

        if year != 2016:
            year = year + 4
        else:
            year = year + 1



        #    print "ValueHistogram:" +  word2count[word]
        #    print '%s\t%s\t%d\tavg is %.6f' % (word, word2count[word], totalCount, word2count[word]/totalCount)
            #print word + ' ' + str(word2count[word]) + 'ValueHistogram:' + word 
            #print "ValueHistogram:" + "word" + "\t" + word2count[word[0]] 
