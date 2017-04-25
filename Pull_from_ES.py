from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

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
	response = es.search(index='tweets',doc_type=like,size=max_num)
	print response['hits']['hits']
	for element in response['hits']['hits']:
		print element['_source']
		L.append(element['_source']['tweet_text'])
		if trigger==1:
			es.delete('tweets',like,element['_id'])
	if len(L)<10:
		response_2 = es.search(index='tweets',doc_type=dislike,size=max_num-len(L))
		for element in response_2['hits']['hits']:
			L.append(element['_source']['tweet_text'])
			if trigger==1:
				es.delete('tweets',dislike,element['_id'])
	print L
	return L
