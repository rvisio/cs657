from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf

from pyspark.mllib.recommendation import ALS
import math
import time
from pyspark.mllib.evaluation import RegressionMetrics, RankingMetrics
from pyspark.mllib.recommendation import Rating

import statistics as s


conf = SparkConf()
conf.setMaster('local[*]')
conf.set('spark.executor.memory', '15G')
conf.set('spark.driver.memory', '15G')
conf.set('spark.driver.maxResultSize', '15G')

sc = SparkContext(conf=conf)
spark = SparkSession(sc)


# In[2]:


# Read in the ratings csv 
ratings = sc.textFile('/Users/robjarvis/cs657/hw4/ml-20m/ratings.csv')
ratings.cache()

#remove the header
header = ratings.first()

ratings = ratings.filter(lambda line: line!=header)

# Split on the commas and then convert each line to a Rating object 
ratings = ratings.map(lambda line: line.split(',')).map(lambda l: Rating(int(l[0]),int(l[1]),float(l[2])))

ratings.cache()

# In[3]:


rank = 10
numIterations = 10

model = ALS.train(ratings, rank, numIterations, nonnegative=True)


# In[4]:


# Calculate RMSE using RegressionMetrics
testData = ratings.map(lambda p: (p.user,p.product))  # remove the rating from test data 
predictions = model.predictAll(testData).map(lambda r: ((r.user,r.product),r.rating))
ratingsTuple = ratings.map(lambda r: ((r.user, r.product), r.rating))
scoreAndLabels = predictions.join(ratingsTuple).map(lambda tup: tup[1])
metrics = RegressionMetrics(scoreAndLabels)
print("RMSE = %s" % metrics.rootMeanSquaredError)
print("MSE = %s" % metrics.meanSquaredError)


# In[4]:


def getMovie(data):
    user = data[0]
    movieTuple = data[1]
    
    movieList = []
    
    for i in range(0, len(movieTuple), 2):
        movieList.append(movieTuple[i])

    
    return user, movieList
    
    


# In[6]:


# RankingMetrics

# Ground truth -- sort movies by movie rating per user id 

# Map the user to a tuple of movie id and movie rating
# Sort the tuple by the rating 
# Reduce by key on the tuple
rankedRatings = ratings.map(lambda r: (r.user, (r.product,r.rating))).sortBy(lambda line: line[1][1], ascending=False).reduceByKey(lambda a,b: a + (b))

# Map the user to the sorted movie order, can remove the rating since no longer needed
groundTruthRankedRatings = rankedRatings.map(getMovie)


# Do the same as above but for the predicted rankings
removeDuplicates = predictions.groupByKey()
rankedTestRatings = removeDuplicates.map(lambda x: (x[0][0], (x[0][1], s.mean(list(x[1]))))).sortBy(lambda line: line[1][1], ascending=False).reduceByKey(lambda a,b: a + (b))
predictedRankedRatings = rankedTestRatings.map(getMovie)


# In[5]:


def convertToRankings(data):
    userId = data[0]
    actualRankings = data[1][0]
    predictedRankings = data[1][1]
    
    rankingDict = {}
    rankingList = []
    predictedList = []
    
    for i in range(0,len(actualRankings)):
        rankingDict[actualRankings[i]] = i+1
        rankingList.append(i+1)
        
    for i in range(0,len(predictedRankings)):
        predictedList.append(rankingDict[predictedRankings[i]])

        
    return (predictedList, rankingList)
                
    


# In[8]:


test = groundTruthRankedRatings.join(predictedRankedRatings)  # joins the rdds on the movie user id 

#user id (tuple ( list of actual rankings, other list of predicted rankings)
rankingsRDD = test.map(convertToRankings)

x = rankingsRDD.map(lambda t: (t[1][0],t[1][1])) # this combines the two rankings 
metrics = RankingMetrics(rankingsRDD)
metrics.meanAveragePrecision


# In[6]:


num_folds = 5
fold1, fold2, fold3, fold4, fold5, = ratings.randomSplit([.2,.2,.2,.2,.2], seed = 9999)
dataList = [fold1,fold2,fold3,fold4,fold5]


for rank in [5,10,15,20]:
    for numIterations in [5,10,15,20]:
        
        print ('rank is ' + str(rank) + '  numIterations is '+ str(numIterations))
        total_RMSE = 0
        total_MAP = 0
        total_MSE = 0 
        for i in range(num_folds):        
            
            currentTesting = dataList[i]
            currentTraining = dataList[:i] + dataList[(i+1):]
            
            train = sc.union(currentTraining)
            test = currentTesting
            

            
        
            model = ALS.train(train,rank,numIterations,nonnegative=True)
        
            testData = test.map(lambda p: (p.user,p.product))  # remove the rating from test data 
            predictions = model.predictAll(testData).map(lambda r: ((r.user,r.product),r.rating))
            ratingsTuple = test.map(lambda r: ((r.user, r.product), r.rating))
            scoreAndLabels = predictions.join(ratingsTuple).map(lambda tup: tup[1])
            metrics = RegressionMetrics(scoreAndLabels)

            
            total_RMSE = total_RMSE + float(metrics.rootMeanSquaredError)
            total_MSE = total_MSE + float(metrics.meanSquaredError)

            # RankingMetrics

            # Ground truth -- sort movies by movie rating per user id 

            # Map the user to a tuple of movie id and movie rating
            # Sort the tuple by the rating 
            # Reduce by key on the tuple
            rankedRatings = test.map(lambda r: (r.user, (r.product,r.rating))).sortBy(lambda line: line[1][1], ascending=False)            .reduceByKey(lambda a,b: a + (b))

            # Map the user to the sorted movie order, can remove the rating since no longer needed
            groundTruthRankedRatings = rankedRatings.map(getMovie)


            # Do the same as above but for the predicted rankings
            removeDuplicates = predictions.groupByKey()
            rankedTestRatings = removeDuplicates.map(lambda x: (x[0][0], (x[0][1], s.mean(list(x[1])))))            .sortBy(lambda line: line[1][1], ascending=False).reduceByKey(lambda a,b: a + (b))
            predictedRankedRatings = rankedTestRatings.map(getMovie)

            test = groundTruthRankedRatings.join(predictedRankedRatings)  # joins the rdds on the movie user id 

            #user id (tuple ( list of actual rankings, other list of predicted rankings)
            rankingsRDD = test.map(convertToRankings)

            metrics = RankingMetrics(rankingsRDD)
            MAP = metrics.meanAveragePrecision   
            
            total_MAP = total_MAP + float(metrics.meanAveragePrecision)
        
        output_rmse = float(total_RMSE)/ float(num_folds)
        output_mse = float(total_MSE)/float(num_folds)
        output_map = float(total_MAP)/float(num_folds)
        print("RMSE = %s" % output_rmse)
        print("MSE = %s" % output_mse)
        print ('MAP equal to ' + str(output_map))
        print ('----------------')
        
        


# In[213]:


jarvis_user_id =0

jarvis_ratings_rdd = sc.textFile('/Users/robjarvis/cs657/hw4/jarvis.csv')

jarvis_ratings = jarvis_ratings_rdd.map(lambda line: line.split(',')).map(lambda l: Rating(float(l[0]),float(l[1]),float(l[2])))


# In[216]:


testRdd = ratings.union(jarvis_ratings)
testRdd.cache()


# In[217]:


new_model = ALS.train(testRdd,rank,numIterations,nonnegative=True)


# In[223]:


testdata = sc.textFile('/Users/robjarvis/cs657/hw4/predict_movies.csv').map(lambda line: line.split(','))

testdata = testdata.map(lambda p: (float(p[0]), float(p[1])))


# In[226]:


predictions = new_model.predictAll(testdata).map(lambda r: ((r[0], r[1]), r[2]))


# In[228]:


predictions.take(10)

