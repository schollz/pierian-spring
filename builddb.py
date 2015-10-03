

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

subreddit_texts = ['python','todayilearned','everythingscience','askscience','askreddit','science','explainlikeimfive']

for subreddit_text in subreddit_texts:
    print "\n\nworking on " + subreddit_text
    conn = sqlite3.connect("ask.db")
    c = conn.cursor()
    c.execute('PRAGMA synchronous = OFF')
    c.execute('PRAGMA journal_mode  = MEMORY')
    try:
        c.execute("""CREATE TABLE submissions(
            rowid INTEGER PRIMARY KEY AUTOINCREMENT,
            id TEXT, 
            subreddit TEXT, 
            created_utc INTEGER, 
            ups INTEGER, 
            downs INTEGER,
            url TEXT)""")
        c.execute("""CREATE VIRTUAL TABLE sub USING FTS4(
            title,
            selftext)""")
    except:
        print "Table already exists"



    numLines = file_len('reddit_submissions/' + subreddit_text + '.json')
    print numLines

    i = 0
    start = time.time()
    cmd = """INSERT INTO submissions (id,subreddit,created_utc,ups,downs,url) 
        VALUES (?,?,?,?,?,?)"""
    cmd2 = """INSERT INTO sub VALUES (?,?)"""
    tuples = []
    tuples2 = []
    num = 0
    with open('reddit_submissions/' + subreddit_text + '.json','r') as f:
        for line in f:
            i+=1
            if i%10000:
                update_progress(i,numLines,start)
            line = line.replace("'","''")
            try:
                a = json.loads(line)
            except:
                a = None
            if a != None and a['ups']>10:
                tuples.append((a['id'],a['subreddit'],a['created_utc'],a['ups'],a['downs'],a['url']))
                tuples2.append((a['title'],a['selftext']))
                num+=1
                if num == 100000:
                    c.executemany(cmd,tuples)
                    tuples = []
                    c.executemany(cmd2,tuples2)
                    tuples2 = []
                    num = 0
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
    num = 0
    with open('reddit_data/' + subreddit_text + '.json','r') as f:
        for line in f:
            i+=1
            if i%10000:
                update_progress(i,numLines,start)
            line = line.replace("'","''")
            try:
                a = json.loads(line)
                a['link_id'] = a['link_id'].split('_')[1]
                a['parent_id'] = a['parent_id'].split('_')[1]
            except:
                a = None
            if a != None and a['ups'] > 3:
                tuples.append((a['link_id'],a['parent_id'],a['subreddit'],a['ups'],a['downs']))
                tuples2.append((a['body'],))
                num+=1
                if num == 100000:
                    c.executemany(cmd,tuples)
                    tuples = []
                    c.executemany(cmd2,tuples2)
                    tuples2 = []
                    num = 0
    if len(tuples)>0:
        c.executemany(cmd,tuples)
        c.executemany(cmd2,tuples2)

    conn.commit()
    conn.close()

print "Creating indicies..."
conn = sqlite3.connect("ask.db")
c = conn.cursor()
c.execute('DROP INDEX IF EXISTS idx1')
c.execute('CREATE INDEX idx1 ON comments (link_id,ups)')
c.execute('DROP INDEX IF EXISTS idx2')
c.execute('CREATE INDEX idx2 ON submissions (id,ups)')
conn.close()


