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
        #categoryPredict = []
        preds = models[0].Predict(testtf)
        preds = preds.map(lambda x:[category[0]]*(int(x)==1) + []*(int(x)==-1))
        for i in range(1,19):
            #i =dictionary[categ]
            #print "w:",models[i].w
            #print "b:",models[i].b
            #print "count_avg:",models[i].count_avg
            preds = preds.zip(models[i].Predict(testtf)).map(lambda x:(x[0]+[category[i]])*(int(x[1])==1)+x[0]*(int(x[1])==-1))
            #if preds.first()==1:
            #        categoryPredict.append(categ)
        #if categoryPredict==[]:
        #    categoryPredict.append("Others")
        preds = preds.map(lambda x: ["Others"]*(not x)+x)
        #print preds.collect()
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
            if average:
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

    def train(self,traindata,trainlabels,source="News"):
        dictionary = self.dictionary
        dictionary["Others"] = -1
        models = self.models
        trainlabels=trainlabels.map(lambda categ: dictionary[categ] )
        for i in range(self.numClasses):
            labelforone = trainlabels.map(lambda x: 1.0*(x==i)+(-1.0)*(x!=i))
            if source =="News":
                models[i].AveragePerceptron(traindata,labelforone)
            elif source =="Feedback":
                models[i].AveragePerceptronFB(traindata,labelforone)
            else:
                print "please choose source from ['News','Feedback']"
        self.models = models
        return models
