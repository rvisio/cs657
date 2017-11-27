#import findspark
#findspark.init()

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



# Read in the ratings csv 
ratings = sc.textFile('/Users/robjarvis/cs657/hw4/ml-20m/ratings.csv')
ratings.cache()

#remove the header
header = ratings.first()

ratings = ratings.filter(lambda line: line!=header)

# Split on the commas and then convert each line to a Rating object 
ratings = ratings.map(lambda line: line.split(',')).map(lambda l: Rating(int(l[0]),int(l[1]),float(l[2])))
ratings.cache()


# In[4]:

# Read in the new user
jarvis_user_id =0

jarvis_ratings_rdd = sc.textFile('/Users/robjarvis/cs657/hw4/jarvis.csv')

jarvis_ratings = jarvis_ratings_rdd.map(lambda line: line.split(',')).map(lambda l: Rating(float(l[0]),float(l[1]),float(l[2])))


# In[5]:


testRdd = ratings.union(jarvis_ratings)
testRdd.cache()


# In[6]:


#rank = 20
#numIterations = 20

rank = 20
numIterations = 20
new_model = ALS.train(testRdd,rank,numIterations, nonnegative=True)


# In[7]:


testdata = sc.textFile('/Users/robjarvis/cs657/hw4/predict_movies.csv').map(lambda line: line.split(','))

testdata = testdata.map(lambda p: (float(p[0]), float(p[1])))


# In[8]:


predictions = new_model.predictAll(testdata).map(lambda r: ((r[0], r[1]), r[2]))


# In[9]:


predictions.coalesce(1).saveAsTextFile('predictions')

