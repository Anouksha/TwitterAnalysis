__author__ = 'anouksha'

import pymongo
import datetime

db = pymongo.MongoClient().tweets
db_parsed = pymongo.MongoClient().TwitterParsed
tweets = db.bharat_phonetweets.find()

print "Starting"

for tweet in tweets:
    #print tweet['created_at']
    date = datetime.datetime.strptime(tweet['created_at'], '%a %b %d %H:%M:%S +0000 %Y')
    print date
    db_parsed.bharat_phonestrain.update({"_id":tweet['id_str']},{"$set":{"created_at":date}})
    #break

print "Done"

#db.collection.find().forEach(function (tweet){db.collection.update({_id: tweet._id}, {$set: {created_at: new Date(tweet.created_at)}});});