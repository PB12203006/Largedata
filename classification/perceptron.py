import numpy as np
from scipy.sparse import coo_matrix


# m sparse matrix
# d dimension
# y label
# m should be preprocessed into a sparse matrix,so currently this function may not work
def Perceptron_train(m,y):
	w=coo_matrix(np.zeros(m.shape[1]))
	b=0
	y = [-1*(x==0 or x==-1)+(x==1) for x in y]
	for i in range (m.shape[0]):
		a=w.dot(m[i].T).todense()+b
		if y[i]*a<=0:
			w=w+y[i]*m[i]
			b=b+y[i]
	return [w,b]


def AveragePerceptron(data, label):
	w = coo_matrix(np.zeros(data.shape[1]))
	u = coo_matrix(np.zeros(data.shape[1]))
	b = 0
	c = 1
	beta = 0
	label = [-1*(x==0 or x==-1)+(x==1) for x in label]
	for i in range(data.shape[0]):
		predict = w.dot(data[i].T).todense() + b
		if label[i]*predict<0 or label[i]*predict==0:
			w = w + label[i]*data[i]
			b = b + label[i]
			u = u + c*label[i]*data[i]
			beta = beta + c*label[i]
		c += 1
	w = w - u/c
	b = b - beta/c
	return [w,b]

def PerceptronPredict(testdata,w,b):
	predict = []
	for i in range(testdata.shape[0]):
		p = w.dot(testdata[i].T).todense() + b
		p = -1*(p<0)+1*(p>=0)
		predict.append(p)
	return predict

class PerceptronforRDD():
    def __init__(self,numFeatures=200):
        self.w = np.zeros(numFeatures)
        self.b = 0
        self.u_avg = np.zeros(numFeatures)
        self.beta_avg = 0
        self.count_avg = 1

    def PerceptronSingle(self,m,y):
        y = y.map(lambda x: -1.0*(x==0.0 or x==-1.0)+(x==1.0))
        pred = m.first().dot(self.w)+self.b
        if y.first()*pred<=0:
            self.w = self.w+y.first()*m.first().toArray()
            self.b = self.b+y.first()
        return [self.w, self.b]

    def PerceptronBatch(self,m,y):
        y = y.map(lambda x: -1.0*(x==0.0 or x==-1.0)+(x==1.0)).collect()
        m = m.collect()
        for i in range(len(m)):
            pred = m[i].dot(self.w)+self.b
            if y[i]*pred<=0:
                self.w = self.w+y[i]*m[i].toArray()
                self.b = self.b+y[i]
        return [self.w, self.b]

    def AveragePerceptron(self, data, label):
        label = label.map(lambda x: -1.0*(x==0.0 or x==-1.0)+(x==1.0))
        label = label.collect()
        data = data.collect()
    	for i in range(len(data)):
    		pred = data[i].dot(self.w) + self.b
    		if label[i]*pred<0 or label[i]*pred==0:
    			self.w = self.w + label[i]*data[i].toArray()
    			self.b = self.b + label[i]
    			self.u_avg = self.u_avg + self.count_avg*label[i]*data[i].toArray()
    			self.beta_avg = self.beta_avg + self.count_avg*label[i]
    		self.count_avg += 1
    	self.w = self.w - self.u_avg/self.count_avg
    	self.beta_avg = self.b - self.beta_avg/self.count_avg
    	return [self.w,self.b]

    def Predict(self,data):
        w = self.w
        b = self.b
        predict = data.map(lambda x: x.dot(w)+b)
        predict = predict.map(lambda p: -1.0*(p<=0)+1.0*(p>0))
        return predict

    def PredictErrrate(self,data,label):
		w = self.w
		b = self.b
		predict = data.map(lambda x: x.dot(w)+b)
		predict = predict.map(lambda p: -1.0*(p<=0)+1.0*(p>0))
		print([predict,predict.count(),predict.first()])
		label = label.map(lambda x: -1.0*(x==0 or x==-1)+1.0*(x==1))
		print([label,label.count(),label.first()])
		ones = predict.zip(label)
		err = ones.map(lambda (x,y): 0*(x==y)+1*(x!=y)).sum()
		#print(err)
		errrate = float(err)/float(label.count())
		return errrate
