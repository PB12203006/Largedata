from flask import Flask, render_template, request
import requests,json
import push
import Pull_from_ES

application = Flask(__name__)
application.config["DEBUG"] = True

@application.route("/",methods=["POST","GET"])
def hello():
	text=['ready?','ready?','ready?','ready?','ready?','ready?','ready?','ready?','ready?','ready?']
	return render_template("hello.html",init_t=text)

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
	for i in range(len(f_A)):
		if txt[i]!='Tooooo fast!!' and txt[i]!='ready?':
			tuple_B.append({'text':txt[i],'label':feedback_B[i]})
			tuple_A.append({'text':txt[i],'category':feedback_A[i]})
	print 'tuple_B',tuple_B,'tuple_B'
	print 'tuple_A',tuple_A,'tuple_A'

	if len(tuple_B)!=0:
		push.push('feedback_B',json.dumps(tuple_B))
		if trigger==1:
			push.push('feedback_A',json.dumps(tuple_A))

	text=Pull_from_ES.pull(str(category),0)
	#text=['x','y']
	myvar={'text':text}
	jvar=json.dumps(myvar)
	return jvar

if __name__ == "__main__":
    application.run(host="0.0.0.0")
