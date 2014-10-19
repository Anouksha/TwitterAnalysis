__author__ = 'anouksha'

import pymongo

db = pymongo.MongoClient().tweets
tweets = db.bharat_phone_stats.find()

for tweet in tweets:
    p_no = ''
    num = tweet['number']
    for n in num:
        if n.isdigit():
            p_no+=n
    tweet['phone_no'] = p_no
    print tweet
    db.bharat_phone_numbers.insert(tweet)

print "Done"
