__author__ = 'anouksha'

import pymongo
import datetime

db = pymongo.MongoClient().TwitterParsed
tweets = db.bharat_phonestrain.find()

print "Starting"

for tweet in tweets:
    print tweet['created_at']
    tweet['created_at'] = datetime.datetime.strptime(tweet['created_at'], '%a %b %d %H:%M:%S +0000 %Y')
    print tweet
    db.bharat_phonestrain.save(tweet)
    break

print "Done"

#db.collection.find().forEach(function (tweet){db.collection.update({_id: tweet._id}, {$set: {created_at: new Date(tweet.created_at)}});});