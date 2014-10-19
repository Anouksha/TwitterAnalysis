__author__ = 'anouksha'

import pymongo
import datetime

db = pymongo.MongoClient().TwitterParsed
tweets = db.bharat_phonestrain.find()

print "Starting"

for tweet in tweets:
    tweet['created_at'] = datetime.datetime.strptime(tweet['created_at'], "%a %b %d %H:%M:%S +0000 %Y")
    print tweet
    db.bharat_phonestrain.save(tweet)

print "Done"