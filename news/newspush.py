'''
push the news to ES
'''
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
	es.index(index=ind,doc_type=category,body=bd,timeout='3000s')
	es.indices.refresh(index=ind)

f_label=open('news_recent_label.txt','r')
f_abstract=open('news_recent_abstract.txt','r')
f_url=open('news_recent_url.txt','r')
f_headline=open('news_recent_headline.txt','r')

while 1==1:
	label=f_label.readline()
	abstract=f_abstract.readline()
	url=f_url.readline()
	headline=f_headline.readline()
	bd={'abstract':abstract,'url':url, 'headline':headline}
	push('news_3',label,bd)
