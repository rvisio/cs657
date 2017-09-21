####README####

graph.png 
    - Contains the runtime vs file size. Runtime is in seconds and file size is in kb

final_output.txt 
    - contains the full output for words that exceed the average and 2 stdev (final requirement from assignment)
    - the output at the end of the string contains the word, count, and year 

mapper.py
    - the mapper used in all jobs.  emits the word, count, year

psuedocode.pdf
    - contains the psuedo code for mapper and the reducers used for jobs

simple_reducer.py
    - simple reducer that performs the word counts and displays an average of the number time the word appears over all inputs

part3_reducer.py
    - displays the average times a word appeared in the year, along with its max and min appearances per address
    - there is a boolean flag that dictates whether to go down the part 3 or part 4 logic. in this file the flag is enabled

part3_sample_output.txt
    - output from part3_reducer.py and mapper.py
    - shows mins for words at 1, 2 and 4


part4_reducer.py 
    - reducer used to display the avg times a word appears in a window and the std dev for a window

sample_output_part4.txt
    - contains the output from mapper.py and part4_reducer.py
    - shows window, the word, the number of times the word appears in a year within the window, total word count from that year, avg times word appears in window, and standard deviation within window

ScrapeSpeeches.py
    - python file to scrape and clean data 

Sample command to run jobs
    time hadoop jar /apps/hadoop-2/share/hadoop/tools/lib/hadoop-streaming-2.4.1.jar -mapper mapper.py -file /home/rjarvis4/mapper.py -reducer reducer.py -file /home/rjarvis4/reducer.py -input /user/rjarvis4/input/* -output /user/rjarvis4/output

Conclusions:
    When checking the change in thresholds from final_output.txt, you can see different words that are triggering the threshold to be exceeded and printed out. In some cases these words are stopper/filler words, but the crux is the more politically charged words that are meeting the threshold.  Certain words that algin closely with a politcal party will appear more often. God exceeded the threshold in 2001, and 2017 (both changes from democrat president to republican.  But also god exceeded the threshold in 2013 (beginning of a democrat presidents second term.) Medicare exceeded the threshold in 2013 (beginning of second term) and immigrant exceeded the threshold in 2017 (major topic of debate during 2016 election)

    Were able to see these changes in a platform through the charged words as a party changes power.
