from flask import Flask, render_template, request
import requests,json

application = Flask(__name__)
application.config["DEBUG"] = True

@application.route("/",methods=["POST","GET"])
def hello():
	return render_template("hello.html")

@application.route("/feedback")
def feedback():

	f=request.args.getlist('f[]')
	y=[int(i) for i in f]
	print y

	text=['Second','Masterpiece','of','Princess','Lan','and','Her','Photographer','Mina','Lisa']

	myvar={"text":text}
	jvar=json.dumps(myvar)
	return jvar

if __name__ == "__main__":
    application.run(host="0.0.0.0")
