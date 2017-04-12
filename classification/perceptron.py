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
