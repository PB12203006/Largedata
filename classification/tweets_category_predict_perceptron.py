import numpy as np
import json
import sys
reload(sys)
sys.setdefaultencoding('UTF8')
from perceptron import PerceptronforRDD
from perceptronMulticlass import MulticlassPerceptron

category = ['Art & Design','World','Sports','Fashion & Style','Books','Music', \
            'Television','Movies','Technology','Science','Food','Real Estate','Theater', \
            'Health','Travel','Education','Your Money','Politics','Economy']
dictionary = {'Art & Design':0,'World':1,'Sports':2,'Fashion & Style':3,'Books':4,'Music':5, \
            'Television':6,'Movies':7,'Technology':8,'Science':9,'Food':10,'Real Estate':11,'Theater':12, \
            'Health':13,'Travel':14,'Education':15,'Your Money':16,'Politics':17,'Economy':18}

def predictTweetCategPerceptron(testtf):
    models = MulticlassPerceptron(numClasses=19,numFeatures=2000,dictionary=dictionary,category=category)
    m = models.load("/Users/Jillian/Documents/Python/large_data_pj/perceptronModels.json")
    pred_categ = models.predict(testtf)
    return pred_categ
