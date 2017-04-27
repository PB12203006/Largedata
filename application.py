from flask import Flask, render_template, request
import requests,json
import push
import Pull_from_ES

application = Flask(__name__)
application.config["DEBUG"] = True

@application.route("/",methods=["POST","GET"])
def hello():
	text=['ready?','ready?','ready?','ready?','ready?','ready?','ready?','ready?','ready?','ready?']
	return render_template("feedback.html",init_t=text)

@application.route("/feedback")
def feedback():
	f_A=request.args.getlist('f_A[]')
	f_B=request.args.getlist('f_B[]')
	t=request.args.getlist('text[]')
	category=request.args.get('category')
	trigger=int(request.args.get('trigger'))
	print 'category',category
	feedback_A=[str(i) for i in f_A]
	feedback_B=[int(i) for i in f_B]
	txt=[str(i) for i in t]
	print 'A',feedback_A
	print 'B',feedback_B
	print 'T',txt
	tuple_A=[]
	tuple_B=[]
	#if feedback_B[0]!=0:
	for i in range(len(f_A)):
		if txt[i]!='Tooooo fast!!' and txt[i]!='ready?':
			if feedback_B[i]!=0:
				tuple_B.append({'text':txt[i],'label':feedback_B[i]})
			tuple_A.append({'text':txt[i],'category':feedback_A[i]})
	print 'tuple_B',tuple_B,'tuple_B'
	print 'tuple_A',tuple_A,'tuple_A'

	if len(tuple_B)!=0:
		if trigger==1 or trigger==3:
			push.push('feedback_B',json.dumps(tuple_B))
	if len(tuple_A)!=0:
		if trigger==2 or trigger==3:
			push.push('feedback_A',json.dumps(tuple_A))
	text=Pull_from_ES.pull(str(category),1)
	myvar={'text':text}
	jvar=json.dumps(myvar)
	return jvar

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
