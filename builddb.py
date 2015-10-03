

import sqlite3
import json
import time
import sys
"""README
database

# submissions

id
subreddit
created_utc
ups
downs
title
selftext


# comments

parent_id (split on '_' - contains id of the submission or comment)
link_id (split both on '_' contains id of the submission)
subreddit
body
ups
downs

"""

subreddit_texts = ['everythingscience','askscience','askreddit','science','explainlikeimfive']

for subreddit_text in subreddit_texts:
    print "working on " + subreddit_text
    conn = sqlite3.connect("ask.db")
    c = conn.cursor()
    c.execute('PRAGMA synchronous = OFF')
    try:
        c.execute("""CREATE TABLE submissions(
            rowid INTEGER PRIMARY KEY AUTOINCREMENT,
            id TEXT, 
            subreddit TEXT, 
            created_utc INTEGER, 
            ups INTEGER, 
            downs INTEGER)""")
        c.execute("""CREATE VIRTUAL TABLE sub USING FTS4(
            title,
            selftext)""")
    except:
        print "Table already exists"

    def file_len(fname):
        with open(fname) as f:
            for i, l in enumerate(f):
                pass
        return i + 1

    def update_progress(progress,total,start):
        try:
            speed = round(1/((time.time()-start)/progress))
        except:
            speed = 0
        sys.stdout.write('\r[{0}] {1}% {2} per/s'.format('#'*(progress*10/total), progress*100/total,speed))
        sys.stdout.flush()

    numLines = file_len('reddit_submissions/' + subreddit_text + '.json')
    print numLines

    i = 0
    start = time.time()
    cmd = """INSERT INTO submissions (id,subreddit,created_utc,ups,downs) 
        VALUES (?,?,?,?,?)"""
    cmd2 = """INSERT INTO sub VALUES (?,?)"""
    tuples = []
    tuples2 = []
    with open('reddit_submissions/' + subreddit_text + '.json','r') as f:
        for line in f:
            i+=1
            update_progress(i,numLines,start)
            line = line.replace("'","''")
            try:
                a = json.loads(line)
            except:
                a = None
            if a != None:
                tuples.append((a['id'],a['subreddit'],a['created_utc'],a['ups'],a['downs']))
                tuples2.append((a['title'],a['selftext']))
                if i % 100000 == 0:
                    c.executemany(cmd,tuples)
                    tuples = []
                    c.executemany(cmd2,tuples2)
                    tuples2 = []
    if len(tuples)>0:
        c.executemany(cmd,tuples)
        c.executemany(cmd2,tuples2)


    print "\nGathering comments\n"
    
    try:
        c.execute("""CREATE TABLE comments(
            rowid INTEGER PRIMARY KEY AUTOINCREMENT,
            link_id TEXT, 
            parent_id TEXT, 
            subreddit TEXT,
            ups INTEGER, 
            downs INTEGER)""")
        c.execute("""CREATE VIRTUAL TABLE com USING FTS4(
            body)""")
    except:
        print "Table already exists"


    numLines = file_len('reddit_data/' + subreddit_text + '.json')
    print numLines

    i = 0
    start = time.time()
    cmd = """INSERT INTO comments (link_id,parent_id,subreddit,ups,downs) 
        VALUES (?,?,?,?,?)"""
    cmd2 = """INSERT INTO com VALUES (?)"""
    tuples = []
    tuples2 = []
    with open('reddit_data/' + subreddit_text + '.json','r') as f:
        for line in f:
            i+=1
            update_progress(i,numLines,start)
            line = line.replace("'","''")
            try:
                a = json.loads(line)
                a['link_id'] = a['link_id'].split('_')[1]
                a['parent_id'] = a['parent_id'].split('_')[1]
            except:
                a = None
            if a != None:
                tuples.append((a['link_id'],a['parent_id'],a['subreddit'],a['ups'],a['downs']))
                tuples2.append((a['body'],))
                if i % 100000 == 0:
                    c.executemany(cmd,tuples)
                    tuples = []
                    c.executemany(cmd2,tuples2)
                    tuples2 = []
    if len(tuples)>0:
        c.executemany(cmd,tuples)
        c.executemany(cmd2,tuples2)
    conn.commit()
    conn.close()