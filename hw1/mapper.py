#!/usr/bin/env python

import sys, string,os 

for line in sys.stdin:
    fileName2 = os.environ["mapreduce_map_input_file"]
    #fileName2 = os.environ["map_file"]
    line = line.strip()
    stripped_line = line.translate(string.maketrans("",""), string.punctuation)

    words = stripped_line.split()
    for word in words:
        print "word " + word  + "\t" + "1" + "\t" + fileName2[-13:-9]
    #        print '%s\t%s' % (word, '1')

