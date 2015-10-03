import json
import time
import sys
import copy
from multiprocessing import Pool, cpu_count

import sqlite3
from collections import OrderedDict
from unidecode import unidecode

print "Working with " + str(cpu_count()) + " processors"
 
def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in xrange(0, len(l), n):
        yield l[i:i+n]

def getComments_worker(ids):
    conn = sqlite3.connect("../ask.db")
    c = conn.cursor()
    comments = []
    if len(ids[0])>1:
        cmd = """select * from comments INDEXED BY idx1 where link_id in (%(comment_id)s) order by ups desc limit 100""" % {'comment_id':"'" + "','".join(ids) + "'"}
    else:
        cmd = """select * from comments INDEXED BY idx1 where link_id=='%(comment_id)s' order by ups desc limit 100""" % {'comment_id':ids}
    
    print cmd
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
            comments.append(comment)
    conn.close()
    return comments


def search_normal(search_text):
    conn = sqlite3.connect("../ask.db")

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

    rowids = []
    for rowid in datas:
        rowids.append(str(rowid))

    cmd = """select * from submissions where rowid in (%(rowids)s) order by ups desc limit 100""" % {'rowids':",".join(rowids)}
    c.execute(cmd)

    for row in c.fetchall():
        rowid = int(row[0])
        datas[rowid]['id'] = row[1]
        datas[rowid]['subreddit'] = row[2]
        datas[rowid]['created_utc'] = row[3]
        datas[rowid]['ups'] = row[4]
        datas[rowid]['downs'] = row[5]
        datas[rowid]['url'] = row[6]
        datas[rowid]['comments'] = []

    print "Got content from submissions in ",
    print time.time()-start
    start = time.time()

    reformated = OrderedDict()
    for rowid in datas:
        if 'id' in datas[rowid]:
            reformated[datas[rowid]['id']] = datas[rowid]
    datas = copy.deepcopy(reformated)
    ids = []
    for id_str in datas:
        ids.append(str(id_str))
    print "Got " + str(len(ids)) + " submissions to get comments from..."

    N = cpu_count()
    ids_partitions = ids #chunks(ids,int(len(ids)/N))
    p = Pool(N)
    for comments in p.map(getComments_worker, ids_partitions):
        for comment in comments:
            datas[comment['link_id']]['comments'].append(comment)
    p.terminate()
    

    print "Got relevant comments in ",
    print time.time()-start
    start = time.time()

    print (len(datas))

    datas2 = copy.deepcopy(datas)
    for data in datas:
        if len(datas[data]['comments'])==0:
            datas2.pop(data)            

    print "Pruned comments in ",
    print time.time()-start
    start = time.time()
    print (len(datas2))

    #print json.dumps(datas,indent=4)
    conn.close()
    return datas2

def inverse_search(search_text):
    conn = sqlite3.connect("../ask.db")

    start = time.time()
    c = conn.cursor()
    cmd = """select rowid,* from com where body match '%(search_string)s'""" % {'search_string':search_text}
    print cmd
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


    cmd = """select * from submissions INDEXED BY idx2 WHERE id in (%(ids)s) and ups > 10 order by ups desc limit 160""" % {'ids':"'" + "','".join(ids) + "'"}
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
        datas[str_id]['url'] = row[6]
        datas[str_id]['comments'] = []
    print "Got info from submissions in ",
    print time.time()-start
    start = time.time()

    ids = newids

    N = cpu_count()
    ids_partitions = ids#chunks(ids,2)
    p = Pool(N)
    for comments in p.map(getComments_worker, ids_partitions):
        for comment in comments:
            datas[comment['link_id']]['comments'].append(comment)
    p.terminate()

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
        if not found or len(datas[data]['comments'])==0:
            print found
            print len(datas[data]['comments'])
            datas2.pop(data)            

    print "Pruned comments in ",
    print time.time()-start
    start = time.time()
    print (len(datas2))


    conn.close()
    return datas2

#search_normal('darwin')
#inverse_search('darwin')