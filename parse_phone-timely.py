#!/usr/bin/python

import re
import datetime
from pymongo import MongoClient
import pymongo
import json
import time
import threading


client = MongoClient()
db = client.TwitterParsed

phonePattern = re.compile(r'([+]?([0-9]+)?[\(.*=$,~ \n\t-]?\s*(\d{3})\s*\)?[\\\/.*=$,~ \n\t-]?\s*\(?(\d{3})\s*\)?[\\\/.*=$,~ \n\t-]?\s*(\d{4})\d*)')

def start(tweet):
    a = phonePattern.findall(tweet['text'])
    p_no = ''
    data = []
    if a:
        print tweet
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


    #if len(data) == 10000:
        try:
            insert2db(data)
        except pymongo.errors.DuplicateKeyError:
            db.bharat_phonestrain.remove({"_id":tweet['_id']})
            insert2db(data)
    #    print len(data), " tweets processed"
    #    data[:] = []


def insert2db(data):
    db.bharat_phonestrain.insert(data)


def run():
    tweets = db_twitter.bharat_phonetweets.find()
    c = 0
    for tweet in tweets:
        #t = threading.Thread(target=start, args=(tweet,))
        #t.daemon = True
        #while threading.activeCount() > 50:
        #    time.sleep(1)
        #t.start()
        #if tweet['id_str'] not in ids:
        #print tweet
        start(tweet)
    #print "ok"
    #print datetime.datetime.now()
    #while threading.activeCount() > 1:
    #    time.sleep(1)
    #    print gcnt, " tweets processed"

    #insert2db()


print "phone parsing starting"
db_twitter = client.tweets
#data = []
#db.phonestrain.drop()
'''phonestrain_tweets = db.bharat_phonestrain.find()
ids = []
for t in phonestrain_tweets:
    ids.append(t['_id'])'''
run()
print "Done parsing..."