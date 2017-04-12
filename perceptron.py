import numpy as np
from scipy.sparse import coo_matrix

# m sparse matrix 
# d dimension
# y label
# m should be preprocessed into a sparse matrix,so currently this function may not work
def Perceptron_train(m,d,y):
	w=coo_matrix(np.zeros(d))
	b=0
	for i in range (m.shape[0]):
		a=w.dot(m[i].T).todense()+b
		if y[i]*a<=0:
			w=w+y[i]*m[i]
			b=b+y[i]
	return [w,b]
