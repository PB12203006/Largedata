from flask import Flask, render_template, request
import requests,json
import push

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
	y=[int(i) for i in f]
	z=[str(i) for i in t]
	print y
	tuple=[]
	for i in range(len(y)):
		tuple.append({'text':z[i],'label':y[i]})
	print tuple

	if tuple[0]['text']!='0':
		push.push(json.dumps(tuple))

	text=['Second','Masterpiece','of','Princess','Lan','and','Her','Photographer','Mina','Lisa']

	myvar={"text":text}
	jvar=json.dumps(myvar)
	return jvar

if __name__ == "__main__":
    application.run(host="0.0.0.0")
