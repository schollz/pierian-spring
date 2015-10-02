import json
import time
from flask import Flask, render_template, request, jsonify


app = Flask(__name__)
app.debug = True

@app.route("/")
def hello():
	a = json.load(open('test.json','r'))
	for key in a:
		a[key]['created_utc'] = time.strftime("%a, %d %b %Y", time.localtime(int(a[key]['created_utc'])))
	# NEED TO CONVERT BODY TEXT FROM MARKDOWN TO HTML
	return render_template('index.html',data=a)


if __name__ == "__main__":
	app.run(host='0.0.0.0',port=8012)
