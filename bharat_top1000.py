__author__ = 'anouksha'

import pymongo

db = pymongo.MongoClient().TwitterParsed

tweets = db.bharat_phone_features.find().sort([("count",pymongo.DESCENDING)]).limit(1000)

for tweet in tweets:
    print tweet['phone_no']+"\t"+tweet['count']

print "Done"