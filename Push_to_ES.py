from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

import json

# change default system encoding method of python into UTF-8
import sys
reload(sys)
sys.setdefaultencoding('UTF8')

#Variables that contains the user credentials to access AWS Elastucsearch
host = '*'
awsauth = AWS4Auth('*', '*', 'us-east-1', 'es')

es = Elasticsearch(
    hosts=[{'host': host, 'port': 443}],
    http_auth=awsauth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection
)


def push(ind,category,bd):
	bd=json.loads(bd)
	es.index(index=ind,doc_type=category,body=bd,timeout='3000s')
	es.indices.refresh(index=ind)
	count = es.count(ind, category)['count']
#if there are too much tweets, delete old ones
	if count >100:
		response = es.search(index=ind,doc_type=category)
		ids = [x["_id"] for x in response["hits"]["hits"]]
		for x in ids[:count-100]:
			es.delete(ind,category,x)
