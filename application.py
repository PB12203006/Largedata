from flask import Flask, render_template, request
import requests,json
import random

application = Flask(__name__)
application.config["DEBUG"] = True

@application.route("/",methods=["POST","GET"])
def hello():
	return render_template("hello.html")

@application.route("/feedback")
def feedback():
	indicator = int(request.args.get('indicator',''))
	print indicator
	t=str(random.randint(0,100))
	myvar={"text":t}
	jvar=json.dumps(myvar)
	return jvar

if __name__ == "__main__":
    application.run(host="0.0.0.0")
