from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import random
import json

# change default system encoding method of python into UTF-8
import sys
reload(sys)
sys.setdefaultencoding('UTF8')

#Variables that contains the user credentials to access AWS Elastucsearch
host = '***REMOVED***'
awsauth = AWS4Auth('***REMOVED***', '***REMOVED***', 'us-east-1', 'es')

es = Elasticsearch(
    hosts=[{'host': host, 'port': 443}],
    http_auth=awsauth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection
)

def pull(category,trigger,max_num=10):
	L=[]
	like=category+'_like'
	dislike=category+'_dislike'
	response = es.search(index='tweets',doc_type=like,size=1000)
	#print response['hits']['hits']
	elements=response['hits']['hits']
	for i in range(min(len(elements), max_num)):
		#print element['_source']
		k=-1-i
		L.append(elements[k]['_source']['tweet_text'])
		if trigger==1:
			es.delete('tweets',like,elements[k]['_id'])
	if len(L)<10:
		response_2 = es.search(index='tweets',doc_type=dislike,size=1000)
		elements_2=response_2['hits']['hits']
		for i in range(min(len(elements_2), max_num-len(L))):
			k=-1-i
			L.append(elements_2[k]['_source']['tweet_text'])
			if trigger==1:
				es.delete('tweets',dislike,elements_2[k]['_id'])
	print L
	return L

def pull_news(category):
	abstract=[]
	url=[]
	headline=[]
	category=category+'\n'
	response = es.search(index='news_3',doc_type=category,size=1000)
	#print response
	elements=response['hits']['hits']
	#print elements
	for i in range(min(len(elements), 10)):
		idx = random.randint(1,len(elements))
		abstract.append(elements[idx]['_source']['abstract'])
		url.append(elements[idx]['_source']['url'])
		headline.append(elements[idx]['_source']['headline'])
	return [abstract,url,headline]