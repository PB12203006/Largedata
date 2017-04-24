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


def push(ind,bd):
	bd=json.loads(bd)
	es.index(index=ind,doc_type="tweet",body=bd)
	es.indices.refresh(index=ind)
	count = es.count(ind, 'tweet')['count']
	if count >100:
		response = es.search(index=ind)
		ids = [x["_id"] for x in response["hits"]["hits"]]
		for x in ids[:count-100]:
			es.delete(ind,'tweet',x)

def pull(key,max_num=10):
	L=[]
	response = es.search(index='tweets_like',doc_type='tweet',size=max_num)
	print response['hits']['hits']
	for element in response['hits']['hits']:
		print element['_source']
		L.append(element['_source']['tweet_text'])
		es.delete('tweets_like','tweet',element['_id'])
	if len(L)<10:
		response_2 = es.search(index='tweets_dislike',doc_type='tweet',size=max_num-len(L))
		for element in response_2['hits']['hits']:
			L.append(element['_source']['tweet_text'])
			es.delete('tweets_dislike','tweet',element['_id'])
	print L
	return L
