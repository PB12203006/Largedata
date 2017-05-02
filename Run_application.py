from flask import Flask, render_template, request
import requests,json
import push_server_to_kafka
import Pull_from_ES

application = Flask(__name__)
application.config["DEBUG"] = True
#mainpage contains all features
@application.route("/",methods=["POST","GET"])
def hello():
	text=['ready?','ready?','ready?','ready?','ready?','ready?','ready?','ready?','ready?','ready?']
	return render_template("feedback.html",init_t=text)

#a route for data exchange
@application.route("/feedback")
def feedback():
#fetch all data in query string
	f_A=request.args.getlist('f_A[]')
	f_B=request.args.getlist('f_B[]')
	t=request.args.getlist('text[]')
	category=request.args.get('category')
	trigger=int(request.args.get('trigger'))
	print 'category',category
#transfer type
	feedback_A=[str(i) for i in f_A]
	feedback_B=[int(i) for i in f_B]
	txt=[str(i) for i in t]
	print 'A',feedback_A
	print 'B',feedback_B
	print 'T',txt
#tuples contain the information that would push to Kafka as feedback
	tuple_A=[]
	tuple_B=[]
#Actually it's always 10 :P
	for i in range(len(f_A)):
#these two conditions means the tweets don't exist so that we abandon the corresponding feedback
		if txt[i]!='Tooooo fast!!' and txt[i]!='ready?':
			#make sure the selector is checked
			if feedback_B[i]!=0:
				tuple_B.append({'text':txt[i],'label':feedback_B[i]})
			tuple_A.append({'text':txt[i],'category':feedback_A[i]})
	print 'tuple_B',tuple_B,'tuple_B'
	print 'tuple_A',tuple_A,'tuple_A'
#decide whether to push feedback depends on trigger
	if len(tuple_B)!=0:
		if trigger==1 or trigger==3:
			push.push('feedback_B',json.dumps(tuple_B))
	if len(tuple_A)!=0:
		if trigger==2 or trigger==3:
			push.push('feedback_A',json.dumps(tuple_A))
#fetch Tweets from ES as response to frontend
	text=Pull_from_ES.pull(str(category),1)
	myvar={'text':text}
	jvar=json.dumps(myvar)
	return jvar
#route below are two simplified clean websites displaying things without feedback function
@application.route('/news',methods=["POST","GET"])
def news():
	return render_template('news.html')

@application.route('/newsrefresh')
def refresh_news():
	category=request.args.get('category')
	print category
	text=Pull_from_ES.pull_news(str(category))
	print text
	myvar={'abstract':text[0],'url':text[1],'headline':text[2]}
	jvar=json.dumps(myvar)
	return jvar

@application.route('/tweets',methods=["POST","GET"])
def tweets():
	return render_template('tweets.html')

@application.route('/tweetsrefresh')
def refresh_tweets():
	category=request.args.get('category')
	print category
	text=Pull_from_ES.pull(str(category),1)
	print text
	myvar={'text':text}
	jvar=json.dumps(myvar)
	return jvar

if __name__ == "__main__":
    application.run(host="0.0.0.0")
