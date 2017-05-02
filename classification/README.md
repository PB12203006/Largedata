### Get news from New York Times
`python news_api.py`


-------
### Compare binary classifiers when number of Features is 200
`/PATH-TO-SPARK/bin/spark-submit  tfidf_classify_spark.py`

__DATA:__ restuarant reviews __{Positive; Negative}__

When number of features is 200:
Compare the confusion matrix and test error rate of models

>{NaiveBayes; SVMWithSGD; LogisticRegressionWithBFGS; OnlinePerceptron(Itr=1); AveragePerceptron(Itr=1);OnlinePerceptron(Itr=1)+AveragePerceptron(Itr=1);AveragePerceptron(Itr=5); AveragePerceptron(Itr=10)}

![Screen Shot 2017-04-26 at 10.04.03 PM.png](quiver-image-url/FB972DDEF4E7CACBD22833704EE80828.png =910x357)



---------
### Compare binary classifiers when number of Features is 2000
`/PATH-TO-SPARK/bin/spark-submit  tfidf_classify_spark.py`

__DATA:__ restuarant reviews __{Positive; Negative}__

When number of features is 2000:
Compare the confusion matrix and test error rate of models
>{NaiveBayes; SVMWithSGD; LogisticRegressionWithBFGS; OnlinePerceptron(Itr=1); AveragePerceptron(Itr=1);OnlinePerceptron(Itr=1)+AveragePerceptron(Itr=1);AveragePerceptron(Itr=5); AveragePerceptron(Itr=10)}

![Screen Shot 2017-04-26 at 10.04.16 PM.png](quiver-image-url/D9EA1B4DA7AA7A012C1C1CF2B7738036.png =912x343)



--------
### MulticlassPerceptron Training error rate and Confusion Matrices:
`/PATH-TO-SPARK/bin/spark-submit  category_news_train_perceptron.py`

__Data__:  News from _NEW YORK TIMES_ in 2016 year, __{number of classes=19; number of features=2000}__

Lifting perceptron binary class models to MulticlassPerceptron using One-vs-Rest

__Training error rate__

![Screen Shot 2017-04-26 at 10.30.14 PM.png](quiver-image-url/1ECE761F895E1E3620983DCF0DC8EFCD.png =732x382)


-----
### Compare Multiclass Models Test error rate and Confusion Matrices:
` /PATH-TO-SPARK/bin/spark-submit compare_Multiclass_Models.py`

__Data__: News from _NEW YORK TIMES_ in 2016 year, __{number of classes=19; number of features=2000}__


![Screen Shot 2017-04-26 at 10.06.37 PM.png](quiver-image-url/00DB4177AED9A09F40E65B46231727F7.png =728x571)



--------
### Compare 4 MulticlassPerceptron Models with different initial weight
`/PATH-TO-SPARK/bin/spark-submit category_tweets_perceptron.py --master local[10]`

Training data is Feedback of correct categories of tweets (200)
Predict categories of test tweets using models after online trained every 50 tweets
- OnlinePerceptron model with weight initialized as zeros
- AveragePerceptron model with weight initialized as zeros
- AveragePerceptron model with weight initialized as trained model by news(but ignore weights history of 22223 news) 
- AveragePerceptron model with weight initialized as trained model by news(and store weights history of 22223 news)

![Screen Shot 2017-04-27 at 12.44.46 AM.png](quiver-image-url/51D08017F1CF1532A43C6C9FACCF2AC3.png =1120x274)

![Screen Shot 2017-04-27 at 12.03.44 AM.png](quiver-image-url/AA97487CCD73DCD84B0ED5B7C86F37A5.png =1197x374)
![Screen Shot 2017-04-27 at 12.04.02 AM.png](quiver-image-url/8CC6AA3907CFAA3778D2ACCA4E289F8B.png =1283x392)
![Screen Shot 2017-04-27 at 12.04.20 AM.png](quiver-image-url/8802B70F486C7BF6A199F8B8A89B9994.png =1234x456)
![Screen Shot 2017-04-27 at 12.05.40 AM.png](quiver-image-url/8F0F9BBC94AE78BFB8577FBE758A2722.png =1183x452)
![Screen Shot 2017-04-27 at 12.05.52 AM.png](quiver-image-url/2359CE16BC686A9D942CF1728891E827.png =1285x452)
![Screen Shot 2017-04-27 at 12.06.06 AM.png](quiver-image-url/A7A0AE7761F4DE5F706F78ED0F2B825F.png =1296x430)



---------
### Compare 4 MulticlassPerceptron Models with different iterations and memory state
`/PATH-TO-SPARK/bin/spark-submit category_tweets_perceptron.py --master local[10]`

Training data is Feedback of correct categories of tweets (400)
Predict categories of test tweets using models after online trained every 100 tweets
- OnlinePerceptron model with weight initialized as zeros, iteration=10
- OnlinePerceptron model with weight initialized as trained model by news(ignored history of News), iteration=10
- AveragePerceptron model with weight initialized as trained model by news(ignore history of News), iteration=1 
- AveragePerceptron model with weight initialized as trained model by news(ignore history of News), iteration=10

![Screen Shot 2017-04-27 at 10.25.59 AM.png](quiver-image-url/1688ACE4A62FE519734D6505DAFAFCB0.png =1207x266)

![Screen Shot 2017-04-27 at 10.16.54 AM.png](quiver-image-url/DB090550CC92F84DEE7F8FEF61D20A28.png =1362x382)
![Screen Shot 2017-04-27 at 4.19.32 PM.png](quiver-image-url/3DC3CCFB3E25638BABC32B18043D77B5.png =1415x689)
![Screen Shot 2017-04-27 at 10.17.28 AM.png](quiver-image-url/20DA3154998A601ECF5EB747F66BCA44.png =1362x462)
![Screen Shot 2017-04-27 at 10.17.42 AM.png](quiver-image-url/05D66CA29B1E18C936F6D525854A08A2.png =1341x456)
![Screen Shot 2017-04-27 at 10.17.56 AM.png](quiver-image-url/87E7689A5DB368DF8B6161BD4209A208.png =1329x455)
![Screen Shot 2017-04-27 at 10.18.08 AM.png](quiver-image-url/6F3E7A2B819A9CE51216BAADD535FF09.png =1352x444)
