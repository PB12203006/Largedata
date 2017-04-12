from sklearn.feature_extraction.text import TfidfVectorizer, CountVectorizer
import csv
import pandas as pd
from sklearn.linear_model import Perceptron

#txt_file = open(r"test_tweets.txt", "rb")
#csv_file = open(r"test_tweets.csv", 'wb')

#in_txt = csv.reader(txt_file, delimiter = '\t')
#out_csv = csv.writer(csv_file)
#out_csv.writerows(in_txt)
#txt_file.close()
#csv_file.close()

reviewdata = open('reviews_tr5000.csv','r')
#print(data0[:2])
data = [x[1] for x in data0[:3000]]
label = [int(x[0]) for x in data0[:3000]]
testdata = [x[1] for x in data0[3000:4000]]
testlabel = [int(x[0]) for x in data0[3000:4000]]
X = [[data[i],label[i]] for i in range(len(label))]
#print(test_tweets)
tfidfvectorizer = TfidfVectorizer()
#print(tfidfvectorizer)
tfvectorizer = CountVectorizer()

tfidf = tfidfvectorizer.fit_transform(data+testdata)
tfidftrain = tfidfvectorizer.fit_transform(data)
tfidftest = tfidfvectorizer.transform(testdata)
print(len(data+testdata))
p1 = Perceptron()
Model_tfidf = p1.fit_transform(tfidftrain,label)
score1 = p1.score(tfidftest,testlabel)
print(score1)

tf = tfvectorizer.fit_transform(data+testdata)
tftrain = tfvectorizer.fit_transform(data)
tftest = tfvectorizer.transform(testdata)
p2 = Perceptron()
Model_tf = p2.fit_transform(tftrain, label)
score2 = p2.score(tftest, testlabel)
print(score2)
reviewdata.close()

#print(tfidf)
#print(tf)
