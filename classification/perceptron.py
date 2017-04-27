import numpy as np
import random
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
    def __init__(self,numFeatures=2000, w=np.zeros(2000),b=0.0,u_avg=np.zeros(2000),beta_avg=0.0,count_avg =1.0):
		if len(w)!= numFeatures:
			self.w = np.zeros(numFeatures)
			self.u_avg = np.zeros(numFeatures)
		else:
			self.w = w
			self.u_avg =u_avg
		self.b = b
		self.beta_avg = beta_avg
		self.count_avg = count_avg

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
		ind = range(len(m))
		random.shuffle(ind)
		for time in range(5):
			random.shuffle(ind)
			for i in ind:
				pred = m[i].dot(self.w)+self.b
				#print "label and predict:",y[i],pred
				if y[i]*pred<=0:
					#print "label and predict:",i,y[i],pred
					self.w = self.w+y[i]*m[i].toArray()
					self.b = self.b+y[i]
		return [self.w, self.b]

    def AveragePerceptronOne(self, data, label):
		label = label.map(lambda x: -1.0*(x==0.0 or x==-1.0)+(x==1.0))
		label = label.collect()
		data = data.collect()
		ind = range(len(data))
		random.shuffle(ind)
		for i in ind:
			pred = data[i].dot(self.w) + self.b
			if label[i]*pred<0 or label[i]*pred==0:
				self.w = self.w + label[i]*data[i].toArray()
				self.b = self.b + label[i]
				self.u_avg = self.u_avg + self.count_avg*label[i]*data[i].toArray()
				self.beta_avg = self.beta_avg + self.count_avg*label[i]
			self.count_avg += 1.0
		self.w = self.w - self.u_avg/self.count_avg
		self.b = self.b - self.beta_avg/self.count_avg
		return [self.w,self.b]

    def AveragePerceptron(self, data, label, MaxItr=3):
		label = label.map(lambda x: -1.0*(x==0.0 or x==-1.0)+(x==1.0))
		label = label.collect()
		#data=data.map(lambda x: x/sum(x))
		data = data.collect()
		ind = range(len(data))
		for time in range(MaxItr):
			random.shuffle(ind)
			for i in ind:
				pred = data[i].dot(self.w) + self.b
				if label[i]*pred<=0:
					#print "label and pred for",i,":",label[i],pred
					#print "data:", data[i].toArray()
					self.w = self.w + label[i]*data[i].toArray()
					#print "b before train:",self.b
					self.b = self.b + label[i]
					#print "b after train:", self.b
					self.u_avg = self.u_avg + self.count_avg*label[i]*data[i].toArray()
					#print "beta_avg before train: ",self.beta_avg
					self.beta_avg = self.beta_avg + self.count_avg*label[i]
					#print "beta_avg after train:", self.beta_avg
				#self.u_avg = self.u_avg + self.w
				#self.beta = self.beta + self.b
				self.count_avg += 1.0
				#print "count number:", self.count_avg
		self.w = self.w - self.u_avg/self.count_avg
		self.b = self.b - self.beta_avg/self.count_avg
		return [self.w,self.b]


    def Predict(self,data):
        w = self.w
        b = self.b
        predict = data.map(lambda x: x.dot(w)+b)
        predict = predict.map(lambda p: -1.0*(p<=0)+1.0*(p>0))
		#print predict.take(10)
        return predict

    def PredictErrrate(self,data,label):
		w = self.w
		b = self.b
		predict = data.map(lambda x: x.dot(w)+b)
		predict = predict.map(lambda p: -1.0*(p<=0)+1.0*(p>0))
		#print(["predict",predict.count(),predict.first()])
		#label = label.map(lambda x: -1.0*(x==0 or x==-1)+1.0*(x==1))
		#print(["label",label.count(),label.first()])
		ones = predict.zip(label)
		err = ones.map(lambda (x,y): 0*(x==y)+1*(x!=y)).sum()
		#print(err)
		errrate = float(err)/float(label.count())
		return errrate

"""
	def AveragePerceptronFB(self, data, label):
		label = label.map(lambda x: -1.0*(x==0.0 or x==-1.0)+(x==1.0))
		label = label.collect()
		data = data.collect()
		ind = range(len(data))
		for time in range(1):
			random.shuffle(ind)
			for i in ind:
				pred = data[i].dot(self.w) + self.b
				if label[i]*pred<=0:
					print "label and pred for",i,":",label[i],pred
					print "data:", data[i].toArray()
					self.w = self.w + label[i]*data[i].toArray()
					print "b before train:",self.b
					self.b = self.b + label[i]
					print "b after train:", self.b
					self.u_avg = self.u_avg + self.count_avg*label[i]*data[i].toArray()
					print "beta_avg before train: ",self.beta_avg
					self.beta_avg = self.beta_avg + self.count_avg*label[i]
					print "beta_avg after train:", self.beta_avg
				self.count_avg += 1.0
				print "count number:", self.count_avg
		self.w = self.w - self.u_avg/self.count_avg
		print "type of b/u_avg/count_avg:", type(self.b), type(self.u_avg), type(self.count_avg)
		self.b = self.b - self.beta_avg/self.count_avg
		return [self.w,self.b]
"""


"""
class PerceptronOVRforDF():
    def __init__(self,numFeatures=20000, numClasses=2,w=[np.zeros(20000)]*2,b=[0]*2):
        if len(w)!=numClasses or len(w[0])!= numFeatures:
            self.w = sc.parallelize([np.zeros(numFeatures)]*numClasses)
            self.b = sc.parallelize([0]*numClasses)
        else:
            self.w = sc.parallelize(w)
            self.b = sc.parallelize(b)
        self.numClasses = numClasses
        self.numFeatures = numFeatures
        self.u_avg = sc.parallelize([np.zeros(numFeatures)]*numClasses)
        self.beta_avg = sc.parallelize([0]*numClasses)
        self.count_avg = 1

    def PerceptronRDDOVR(self,dataset):
        numclasses = self.numClasses
        y = dataset.select('label').rdd.map(lambda row: row.label) \
            .map(lambda x: [-1]*int(x)+[1]+[-1]*(numclasses-1-int(x))).collect()
        m = dataset.select('features').rdd.map(lambda row: row.features).collect()
        w = self.w.collect()
        b = self.b.collect()
        for i in range(len(m)):
            preds = [m[i].dot(w[j])+b[j] for j in range(numclasses)]
            err = [preds[j]*y[i][j]<=0 for j in range(numclasses)]
            w = [(w[j]+y[i][j]*m[i].toArray())*err[j]+w[j]*(not(err[j])) for j in range(numclasses)]
            b = [(b[j]+y[i][j])*err[j]+b[j]*(not(err[j])) for j in range(numclasses)]
        self.w = sc.parallelize(w)
        self.b = sc.parallelize(b)
        return [self.w, self.b]

    def PerceptronBatchOVR(self,dataset):
        numclasses = self.numClasses
        y = dataset.select('label').rdd.map(lambda row: row.label) \
            .map(lambda x: [-1]*int(x)+[1]+[-1]*(numclasses-1-int(x))).collect()
        #y = sc.broadcast(y)
        m = dataset.select('features').rdd.map(lambda row: row.features).collect()
        w = self.w
        b = self.b
        for i in range(len(m)):
            preds = b.zip(w).map(lambda x: m[i].dot(x[1])+x[0])
            err = preds.zip(sc.parallelize(y[i])).map(lambda x: [x[0]*x[1]<=0,x[1]])
            #w = err.zip(w).map(lambda p: (p[1]+p[0][1]*m.value[i].toArray())*p[0][0]+p[1]*(not(p[0][0])))
            w = err.zip(w).map(lambda p: (p[1]+p[0][1]*m[i].toArray())*p[0][0]+p[1]*(not(p[0][0])))
            b = err.zip(b).map(lambda p: (p[1]+p[0][1])*p[0][0]+p[1]*(not(p[0][0])))
            #b = err.zip(b).map(lambda p: (p[1]+p[0][1])*p[0][0]+p[1]*(not(p[0][0])))
        self.w = w
        self.b = b
        return [self.w, self.b]

    def Predict(self,data):
        w = self.w.collect()
        b = self.b.collect()
        num = self.numClasses
        pred = data.map(lambda x: [x.dot(w[i])+b[i] for i in range(num)])
        predict = pred.map(lambda p: [i for i in range(num) if p[i]>0])
        #predict = predict.map(lambda p: -1.0*(p<0)+1.0*(p>=0))
        return predict
"""
