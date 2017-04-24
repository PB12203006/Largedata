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
	f=request.args.getlist('f[]')
	t=request.args.getlist('text[]')
	category=request.args.get('category')
	print 'category',category
	y=[int(i) for i in f]
	z=[str(i) for i in t]
	print y
	tuple=[]
	for i in range(len(y)):
		if z[i]!='Tooooo fast!!' and z[i]!='ready?':
			tuple.append({'text':z[i],'label':y[i]})
	print tuple

	if len(tuple)!=0:
		push.push(json.dumps(tuple))

	text=Pull_from_ES.pull(str(category))
	#text=['x','y']
	myvar={'text':text}
	jvar=json.dumps(myvar)
	return jvar

if __name__ == "__main__":
    application.run(host="0.0.0.0")
