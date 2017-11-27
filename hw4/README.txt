Directory Structure

    Psuedocode:
        MetricCalculation_And_CrossValidation
            - Psuedocode for calculating RegressionMetrics and RankingMetrics and implementing GridSearch + five fold cross validation
        Prediction  
            - Psuedocode for training a model and making predictions on the test user data 
    
    Code 
        cross_validation_metrics.py
            - Code for cross validation and calculating metrics

        predictions.py
            - Code for training model and making predictions on the test data set
         

    Results
        - cross_validation.txt
            - Results from running cross validation and metric calculation

        - predictions.txt
            - Predictions made by the model on the test user data

        - predict_movies.csv + jarvis.csv
            The input files used for testing my own likes/dislikes in the recommendation system


------Conclusion----
Jupyter notebook was used for most of the development process, it was a very helpful tool for running code and quickly checking outputs. 

I was unable to run these models locally on my script when using the large 20 million dataset.  I spun up an aws m4.10xlarge instance for running the models on those datasets. Caching large RDD's and increasing the spark executor memory within the spark-conf files  helped resolve the issue, but in the future more attention may need to be paid to the way RDD's are being constructed and operated on,

I also noticed that sometimes there would be duplicate records within my RDD.  A userId, movieId and rating would be exactly repeated multiple times. To resolve this issue, I grouped by key on the userId and movieId and then converted the pypskar ResultIterable to a list and averaged the result.

Finally, the predictions were nothing special.  Looking at the outputted predictions I was seeing negative numbers included.  After checking around I came across a parameter "nonnegative" that I could pass in to ALS.train.  After passing that in my predictions looked better, no negative numbers included in the predictions.  The movies I attempted to predicted on were all movies I do like, and the predictions came back as favorable.  One movie, "The Lego Movie" is a childrens movie and it was rated the lowest.  That seems consistent with the ratings provided as not many chilrens movies were included for the new user dataset


The data (my own predictions and sample data is provided within the results directory)
