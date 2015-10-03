import json
import time
import sys
import copy

import sqlite3
from collections import OrderedDict
from unidecode import unidecode

def search(search_text):
	conn = sqlite3.connect("test.db")

	start = time.time()
	datas = {}
	c = conn.cursor()
	cmd = """select rowid,* from sub  where title match '%(search_string)s'""" % {'search_string':search_text}
	print cmd
	for row in c.execute(cmd):
		datas[row[0]] = {}
		datas[row[0]]['title'] = row[1]
		datas[row[0]]['selftext'] = row[2]

	print "Matched submissions from title in ",
	print time.time()-start
	start = time.time()

	cmd = """select rowid,* from sub  where selftext match '%(search_string)s'""" % {'search_string':search_text}
	print cmd
	for row in c.execute(cmd):
		datas[row[0]] = {}
		datas[row[0]]['title'] = row[1]
		datas[row[0]]['selftext'] = row[2]

	print "Matched submissions from selftext in ",
	print time.time()-start
	start = time.time()


	for rowid in datas:
		cmd = """select * from submissions where rowid = %(rowid)s""" % {'rowid':rowid}
		for row in c.execute(cmd):
			datas[rowid]['id'] = row[1]
			datas[rowid]['subreddit'] = row[2]
			datas[rowid]['created_utc'] = row[3]
			datas[rowid]['ups'] = row[4]
			datas[rowid]['downs'] = row[5]
			datas[rowid]['comments'] = []

	print "Got content from submissions in ",
	print time.time()-start
	start = time.time()

	reformated = {}
	for rowid in datas:
		reformated[datas[rowid]['id']] = datas[rowid]
	datas = reformated

	ids = []
	for id_str in datas:
		ids.append(str(id_str))


	cmd = """select * from comments where link_id in (%(comment_id)s)""" % {'comment_id':"'" + "','".join(ids) + "'"}

	c.execute(cmd)
	for row in c.fetchall():
		comment = {}
		comment['rowid'] = row[0]
		comment['link_id'] = row[1]
		comment['parent_id'] = row[2]
		comment['subreddit'] = row[3]
		comment['ups'] = row[4]
		comment['downs'] = row[5]
		cmd2 = """select * from com where rowid = %(row_id)s""" % {'row_id':row[0]}
		c.execute(cmd2)
		for row2 in c.fetchone():
			comment['body'] = row2
		datas[comment['link_id']]['comments'].append(comment)

	print "Got relevant comments in ",
	print time.time()-start
	start = time.time()

	#print json.dumps(datas,indent=4)
	conn.close()
	return datas

def inverse_search(search_text):
	conn = sqlite3.connect("test.db")

	start = time.time()
	c = conn.cursor()
	cmd = """select rowid,* from com where body match '%(search_string)s'""" % {'search_string':search_text}
	rowids = []
	for row in c.execute(cmd):
		rowids.append(str(row[0]))

	rowids = list(set(rowids))

	print "Matched comments from body in ",
	print time.time()-start
	start = time.time()

	ids = []
	cmd = """select link_id from comments where rowid in (%(rowids)s)""" % {'rowids':",".join(rowids)}
	for row in c.execute(cmd):
		ids.append(row[0])

	print "Got link_ids from comments in ",
	print time.time()-start
	start = time.time()


	cmd = """select * from submissions where id in (%(ids)s) order by ups desc limit 100""" % {'ids':"'" + "','".join(ids) + "'"}
	datas = OrderedDict()
	newids = []
	c.execute(cmd)
	for row in c.fetchall():
		str_id = row[1]
		datas[str_id] = OrderedDict()
		cmd2 = """select * from sub where rowid = %(row_id)s""" % {'row_id':row[0]}
		c.execute(cmd2)
		for row2 in c.fetchall():
			datas[str_id]['title'] = row2[0]
			datas[str_id]['selftext'] = row2[1]
		newids.append(str_id)
		datas[str_id]['subreddit'] = row[2]
		datas[str_id]['created_utc'] = row[3]
		datas[str_id]['ups'] = row[4]
		datas[str_id]['downs'] = row[5]
		datas[str_id]['comments'] = []
	print "Got info from submissions in ",
	print time.time()-start
	start = time.time()

	ids = newids
	cmd = """select * from comments where link_id in (%(comment_id)s) and ups > 10 order by ups desc""" % {'comment_id':"'" + "','".join(ids) + "'"}

	c.execute(cmd)
	for row in c.fetchall():
		comment = {}
		comment['rowid'] = row[0]
		comment['link_id'] = row[1]
		comment['parent_id'] = row[2]
		comment['subreddit'] = row[3]
		comment['ups'] = row[4]
		comment['downs'] = row[5]
		cmd2 = """select * from com where rowid = %(row_id)s""" % {'row_id':row[0]}
		c.execute(cmd2)
		for row2 in c.fetchone():
			comment['body'] = row2
		if "[deleted]" != comment['body']:
			datas[comment['link_id']]['comments'].append(comment)

	print "Got relevant comments in ",
	print time.time()-start
	start = time.time()

	print (len(datas))

	datas2 = copy.deepcopy(datas)
	for data in datas:
		found = False
		for i in range(len(datas[data]['comments'])):
			if search_text in datas[data]['comments'][i]['body']:
				found = True
		if not found:
			datas2.pop(data)			

	print "Pruned comments in ",
	print time.time()-start
	start = time.time()
	print (len(datas2))


	conn.close()
	return datas2

