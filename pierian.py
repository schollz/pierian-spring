import json
import time
from flask import Flask, render_template, request, jsonify
import markdown2

from searchdb import *

app = Flask(__name__)
app.debug = True

@app.route("/", methods=['POST','GET'])
def search():
	a = {}
	if request.method == 'POST':
		print request.form['searchText']
		if 'True' in request.form.getlist('inverseSearch'):
			a = inverse_search(request.form['searchText'])
		else:
			a = search_normal(request.form['searchText'])

		for key in a:
			a[key]['created_utc'] = time.strftime("%a, %d %b %Y", time.localtime(int(a[key]['created_utc'])))
			a[key]['selftext'] = markdown2.markdown(a[key]['selftext'].replace("''","'"))
			a[key]['title'] = a[key]['title'].replace("''","'")
			for i in range(len(a[key]['comments'])):
				a[key]['comments'][i]['body'] = markdown2.markdown(a[key]['comments'][i]['body'].replace("''","'"))
	return render_template('index.html',data=a)


if __name__ == "__main__":
    while True:
        try:
            app.run(host='0.0.0.0',port=8012)
        except:
            pass
