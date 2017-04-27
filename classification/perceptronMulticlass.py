from perceptron import PerceptronforRDD
import numpy as np
import json
import sys
reload(sys)
sys.setdefaultencoding('UTF8')

class MulticlassPerceptron():
    def __init__(self,numClasses=19, numFeatures=2000,dictionary={},category=[]):
        self.numClasses=numClasses
        self.models=[PerceptronforRDD(numFeatures=2000)]*numClasses
        self.dictionary = dictionary
        self.category = category


    def predict(self,testtf):
        category = self.category
        models = self.models
        numclass = self.numClasses
        categoryPredict = []
        preds = []
        for i in range(numclass):
            preds.append(models[i].Predict(testtf).collect())
        #print "\nafter for loop\n",preds[:5]
        for j in range(len(preds[0])):
            categoryPredict.append([category[x] for x in range(19) if preds[x][j]==1])
            if categoryPredict[j]==[]:
                categoryPredict[j] = ["Others"]
        #print "\nafter 2nd for loop\n",categoryPredict[:5]
        return categoryPredict

    def predict0(self,testtf):
        category = self.category
        models = self.models
        #categoryPredict = []
        preds = models[0].Predict(testtf)
        preds = preds.map(lambda x:[0,category[0]]*(int(x)==1) + [0]*(int(x)==-1))
        #print "\nthe 0 time in for loop",preds.collect()
        for i in range(1,19):
            #print "\nmodel ",i,preds.collect()
            preds = preds.zip(models[i].Predict(testtf))
            #print "\nmodel after zip:",i,preds.collect()
            preds = preds.map(lambda x:(x[0]+[category[i]])*(int(x[1])==1)+x[0]*(int(x[1])==-1))
            #print "\nthe",i,"time in for loop", preds.collect()
        preds = preds.map(lambda x: ["Others"]*(x[-1]==0)+x[1:]*(x[-1]!=0))
        #print "\nafter for loop\n",preds.take(10)
        return preds.collect()

    def load(self,path_json,average=True):
        dictionary = self.dictionary
        with open(path_json) as data_file:
            models_param = json.load(data_file)
        perceptronModels = range(self.numClasses)
        self.numClasses=len(dictionary.keys())
        category = self.category
        #print "load category keys:", dictionary.keys()
        for categ in dictionary.keys():
            param = models_param[categ]
            if average==True:
                perceptronModels[dictionary[categ]]=PerceptronforRDD(w=np.array(param['w']),b=param['b'],u_avg=np.array(param['u_avg']),beta_avg=param['beta_avg'],count_avg=param['count_avg'])
            else:
                perceptronModels[dictionary[categ]]=PerceptronforRDD(w=np.array(param['w']),b=param['b'])
        self.models = perceptronModels
        #print "number of load models:",len(perceptronModels)
        return self.models

    def save(self,path_json):
        #json_file = "perceptronModels.json"
        models = self.models
        category = self.category
        model_param = {}
        for i in range(self.numClasses):
            parameters = {"w":models[i].w.tolist(),"b":models[i].b,"u_avg":models[i].u_avg.tolist(),"beta_avg":models[i].beta_avg,"count_avg":models[i].count_avg}
            model_param[category[i]]=parameters
        with open(path_json, 'w') as outfile:
            json.dump(model_param, outfile)
        print "write models parameters to file complete! path:",path_json

    def train(self,traindata,trainlabels,method="Average",source="Feedback", MaxItr=10):
        dictionary = self.dictionary
        #dictionary["Others"] = -1
        models = self.models
        def mapDict(categ):
            if categ == "Others":
                return -1
            else:
                return dictionary[categ]
        #print "training labels", trainlabels.collect()
        trainlabels=trainlabels.map(mapDict)
        #print "training labels after map", trainlabels.collect()
        for i in range(self.numClasses):
            labelforone = trainlabels.map(lambda x: 1.0*(x==i)+(-1.0)*(x!=i))
            #print "training model",i,labelforone.collect()
            if method == "Online":
                models[i].PerceptronBatch(traindata,labelforone,MaxItr=MaxItr)
            elif method =="Average":
                #models[i].PerceptronBatch(traindata,labelforone)
                models[i].AveragePerceptron(traindata,labelforone,MaxItr=MaxItr)
            else:
                print "please choose source from ['News','Feedback']"
        self.models = models
        return models
