from sklearn.feature_extraction.text import TfidfVectorizer, CountVectorizer
import csv
import pandas as pd
from sklearn.linear_model import Perceptron
from perceptron import Perceptron_train, AveragePerceptron, PerceptronPredict
import numpy as np

#txt_file = open(r"test_tweets.txt", "rb")
#csv_file = open(r"test_tweets.csv", 'wb')

#in_txt = csv.reader(txt_file, delimiter = '\t')
#out_csv = csv.writer(csv_file)
#out_csv.writerows(in_txt)
#txt_file.close()
#csv_file.close()

reviews = open('reviews_tr5000.txt', 'r')
reviewlabel = open('reviews_tr5000_label.txt', 'r')
#data0 = reviews.map(lambda line: line.split(','))
data0 = reviews.readlines()
labels = reviewlabel.readlines()
reviews.close()
reviewlabel.close()
data = [x.split() for x in data0[1:3000]]
label = [int(x) for x in labels[1:3000]]
testdata = [x.split() for x in data0[3000:4000]]
testlabel = [int(x) for x in labels[3000:4000]]
X = [[data[i],label[i]] for i in range(len(label))]
tfidfvectorizer = TfidfVectorizer()
#print(tfidfvectorizer)
tfvectorizer = CountVectorizer()

tfidf = tfidfvectorizer.fit_transform(data0[1:4000])
tfidftrain = tfidfvectorizer.fit_transform(data0[1:3000])
tfidftest = tfidfvectorizer.transform(data0[3000:4000])
print(len(data+testdata))

#print(tfidf.shape)
dim = tfidftrain.shape[1]
dimtest = tfidftest.shape[1]
p1 = Perceptron()
Model_tfidf = p1.fit_transform(tfidftrain,label)
score1 = p1.score(tfidftest,testlabel)
print(score1)

tf = tfvectorizer.fit_transform(data0[1:4000])
tftrain = tfvectorizer.fit_transform(data0[1:3000])
tftest = tfvectorizer.transform(data0[3000:4000])
p2 = Perceptron()
Model_tf = p2.fit_transform(tftrain, label)
score2 = p2.score(tftest, testlabel)
print(score2)


[w,b] = Perceptron_train(tfidftrain, label)
predict = PerceptronPredict(tfidftest, w, b)
testlabel = [-1*(x==0 or x==-1)+(x==1) for x in testlabel]
ones = [1*(testlabel[i]!=predict[i]) for i in range(len(testlabel))]
err = sum(ones)
print(err)
print(err[0])
errrate = float(err)/float(len(testlabel))
print(errrate)
[w,b] = AveragePerceptron(tfidftrain, label)
predict = PerceptronPredict(tfidftest, w, b)
testlabel = [-1*(x==0 or x==-1)+(x==1) for x in testlabel]
ones = [1*(testlabel[i]!=predict[i]) for i in range(len(testlabel))]
err = sum(ones)
print(err)
errrate = float(err)/float(len(testlabel))
print(errrate)
