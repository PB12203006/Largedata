import numpy as np
import json
import sys
reload(sys)
sys.setdefaultencoding('UTF8')
from perceptron import PerceptronforRDD

category = ['Art & Design','World','Sports','Fashion & Style','Books','Music', \
            'Television','Movies','Technology','Science','Food','Real Estate','Theater', \
            'Health','Travel','Education','Your Money','Politics','Economy']
dictionary = {'Art & Design':0,'World':1,'Sports':2,'Fashion & Style':3,'Books':4,'Music':5, \
            'Television':6,'Movies':7,'Technology':8,'Science':9,'Food':10,'Real Estate':11,'Theater':12, \
            'Health':13,'Travel':14,'Education':15,'Your Money':16,'Politics':17,'Economy':18}

def predictTweetCategPerceptron(testtf):
    with open("perceptronModels.json") as data_file:
        models_para = json.load(data_file)
    perceptronModels = {}
    categoryPredict = []
    for categ in dictionary.keys():
        param = models_para[categ]
        perceptronModels[categ]=PerceptronforRDD(w=np.array(param['w']),b=param['b'],u_avg=np.array(param['u_avg']),beta_avg=param['beta_avg'],count_avg=param['count_avg'])
        preds = perceptronModels[categ].Predict(testtf)
        if preds.first()==1:
            categoryPredict.append(categ)
    return categoryPredict
