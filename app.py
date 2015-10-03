import json
import time
from flask import Flask, render_template, request, jsonify
import markdown2

app = Flask(__name__)
app.debug = True

@app.route("/")
def hello():
	a = {}
	return render_template('index.html',data=a)


@app.route("/search", methods=['POST'])
def search():
	a = json.load(open('test.json','r'))
	for key in a:
		a[key]['created_utc'] = time.strftime("%a, %d %b %Y", time.localtime(int(a[key]['created_utc'])))
		a[key]['selftext'] = markdown2.markdown(a[key]['selftext'].replace("''","'"))
		for i in range(len(a[key]['comments'])):
			a[key]['comments'][i]['body'] = markdown2.markdown(a[key]['comments'][i]['body'].replace("''","'"))
	return render_template('index.html',data=a)


if __name__ == "__main__":
	app.run(host='0.0.0.0',port=8012)
