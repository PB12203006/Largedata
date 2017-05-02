### Get news from New York Times
`python news_api.py`


-------
### Compare binary classifiers when number of Features is 200
`/PATH-TO-SPARK/bin/spark-submit  tfidf_classify_spark.py`

__DATA:__ restuarant reviews __{Positive; Negative}__

When number of features is 200:
Compare the confusion matrix and test error rate of models

>{NaiveBayes; SVMWithSGD; LogisticRegressionWithBFGS; OnlinePerceptron(Itr=1); AveragePerceptron(Itr=1);OnlinePerceptron(Itr=1)+AveragePerceptron(Itr=1);AveragePerceptron(Itr=5); AveragePerceptron(Itr=10)}

![binary_200features.png](https://github.com/PB12203006/Largedata/blob/master/classification/pic/binary_200features.png)



---------
### Compare binary classifiers when number of Features is 2000
`/PATH-TO-SPARK/bin/spark-submit  tfidf_classify_spark.py`

__DATA:__ restuarant reviews __{Positive; Negative}__

When number of features is 2000:
Compare the confusion matrix and test error rate of models
>{NaiveBayes; SVMWithSGD; LogisticRegressionWithBFGS; OnlinePerceptron(Itr=1); AveragePerceptron(Itr=1);OnlinePerceptron(Itr=1)+AveragePerceptron(Itr=1);AveragePerceptron(Itr=5); AveragePerceptron(Itr=10)}

![binary_2000features.png](https://github.com/PB12203006/Largedata/blob/master/classification/pic/binary_2000features.png)



--------
### MulticlassPerceptron Training error rate and Confusion Matrices:
`/PATH-TO-SPARK/bin/spark-submit  category_news_train_perceptron.py`

__Data__:  News from _NEW YORK TIMES_ in 2016 year, __{number of classes=19; number of features=2000}__

Lifting perceptron binary class models to MulticlassPerceptron using One-vs-Rest

__Training error rate__

![multiclass_training_confusionMatrix.png](https://github.com/PB12203006/Largedata/blob/master/classification/pic/multiclass_training_confusionMatrix.png)


-----
### Compare Multiclass Models Test error rate and Confusion Matrices:
` /PATH-TO-SPARK/bin/spark-submit compare_Multiclass_Models.py`

__Data__: News from _NEW YORK TIMES_ in 2016 year, __{number of classes=19; number of features=2000}__


![multiclass_compare_test_confusionMatrix.png](https://github.com/PB12203006/Largedata/blob/master/classification/pic/multiclass_compare_test_confusionMatrix.png)



--------
### Compare 4 MulticlassPerceptron Models with different initial weight
`/PATH-TO-SPARK/bin/spark-submit category_tweets_perceptron.py --master local[10]`

Training data is Feedback of correct categories of tweets (200)
Predict categories of test tweets using models after online trained every 50 tweets
- OnlinePerceptron model with weight initialized as zeros
- AveragePerceptron model with weight initialized as zeros
- AveragePerceptron model with weight initialized as trained model by news(but ignore weights history of 22223 news) 
- AveragePerceptron model with weight initialized as trained model by news(and store weights history of 22223 news)

![multiclass_code_compare_initial.png](https://github.com/PB12203006/Largedata/blob/master/classification/pic/multiclass_200/multiclass_code_compare_initial.png)

![tweets_Category.png](https://github.com/PB12203006/Largedata/blob/master/classification/pic/multiclass_200/tweets_Category.png)

![multiclass_compare_initial_0.png](https://github.com/PB12203006/Largedata/blob/master/classification/pic/multiclass_200/multiclass_compare_initial_0.png)
![multiclass_compare_initial_1.png](https://github.com/PB12203006/Largedata/blob/master/classification/pic/multiclass_200/multiclass_compare_initial_1.png)
![multiclass_compare_initial_2.png](https://github.com/PB12203006/Largedata/blob/master/classification/pic/multiclass_200/multiclass_compare_initial_2.png)
![multiclass_compare_initial_3.png](https://github.com/PB12203006/Largedata/blob/master/classification/pic/multiclass_200/multiclass_compare_initial_3.png)
![multiclass_compare_initial_final.png](https://github.com/PB12203006/Largedata/blob/master/classification/pic/multiclass_200/multiclass_compare_initial_final.png)



---------
### Compare 4 MulticlassPerceptron Models with different iterations and memory state
`/PATH-TO-SPARK/bin/spark-submit category_tweets_perceptron.py --master local[10]`

Training data is Feedback of correct categories of tweets (400)
Predict categories of test tweets using models after online trained every 100 tweets
- OnlinePerceptron model with weight initialized as zeros, iteration=10
- OnlinePerceptron model with weight initialized as trained model by news(ignored history of News), iteration=10
- AveragePerceptron model with weight initialized as trained model by news(ignore history of News), iteration=1 
- AveragePerceptron model with weight initialized as trained model by news(ignore history of News), iteration=10

![multiclass_code_400.png](https://github.com/PB12203006/Largedata/blob/master/classification/pic/multiclass_400/multiclass_code_400.png)

![tweets_category.png](https://github.com/PB12203006/Largedata/blob/master/classification/pic/multiclass_400/tweets_category.png)

![multiclass_400_compare_0.png](https://github.com/PB12203006/Largedata/blob/master/classification/pic/multiclass_400/multiclass_400_compare_0.png)
![multiclass_400_compare_1.png](https://github.com/PB12203006/Largedata/blob/master/classification/pic/multiclass_400/multiclass_400_compare_1.png)
![multiclass_400_compare_2.png](https://github.com/PB12203006/Largedata/blob/master/classification/pic/multiclass_400/multiclass_400_compare_2.png)
![multiclass_400_compare_3.png](https://github.com/PB12203006/Largedata/blob/master/classification/pic/multiclass_400/multiclass_400_compare_3.png)
![multiclass_400_compare_final.png](https://github.com/PB12203006/Largedata/blob/master/classification/pic/multiclass_400/multiclass_400_compare_final.png)

