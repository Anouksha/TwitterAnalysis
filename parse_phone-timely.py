#!/usr/bin/python

import re
import datetime
from pymongo import MongoClient


client = MongoClient()
db = client.TwitterParsed

phonePattern = re.compile(r'([+]?([0-9]+)?[\(.*=$,~ \n\t-]?\s*(\d{3})\s*\)?[\\\/.*=$,~ \n\t-]?\s*\(?(\d{3})\s*\)?[\\\/.*=$,~ \n\t-]?\s*(\d{4})\d*)')

def start(tweet):
    #print "here in start"
    #gcnt = gcnt + 1
    #print tweet
    a = phonePattern.findall(tweet['text'])
    p_no = ''
    arr = []
    if a:
        for i in a[0][0]:
            if i.isdigit():
                p_no += i
        tweet['phone_no'] = p_no
        tweet['o_pno'] = a[0][0].strip()
        
        tweet['url_check'] = []
        for fields in db.resolve_urls.find({"tweet_id": tweet['id_str']}):
            tweet['url_check'].append(fields)
        tweet['_id'] = tweet['id_str']
        data.append(tweet)
        #{'phone_no': p_no, 'id': tweet['id_str'], 'o_pno' : a[0][0].strip()})

    if len(data) == 10000:
        insert2db()
        print len(data), " tweets processed"
        data[:] = []


def insert2db():
    #print "here insert2db", len(data)
    db.phonestrain.insert(data)


def run_between_twodays():
    #today = datetime.datetime.now()
    print datetime.datetime.now()
    t1 = datetime.datetime(2014,7,1,0,0,0)
    t2 = datetime.datetime(2014,9,20,0,0,0)
    print t1, t2
    tweets = db_twitter.tweets.find({"created_at": {"$gte": t1, "$lte": t2}}, batch_size=10000)
    #tweets = tweets[:]
    print "done tweets"
    #c=0
    for tweet in tweets:
        #c=1
        #t = threading.Thread(target=start, args=(tweet,))
        #t.daemon = True
        #while threading.activeCount() > 50:
        #    time.sleep(1)
        #t.start()
        #print tweet
        start(tweet)
    print "ok"
    print datetime.datetime.now()
    #while threading.activeCount() > 1:
    #    time.sleep(1)
    #    print gcnt, " tweets processed"

    insert2db()


print "phone parsing starting"
db_twitter = client.Twitter
data = []
db.phonestrain.drop()
run_between_twodays()