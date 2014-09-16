__author__ = 'anouksha'

import pymongo
from sets import Set
import sys

db = pymongo.MongoClient().Twitter
db_parsed = pymongo.MongoClient().TwitterParsed

tweets = db.tweets.find().limit(200)
accounts_ids = {}

for tweet in tweets:
    name = tweet['user']['screen_name']
    tweet_ids = []
    if name not in accounts_ids:
        tweet_ids.append(tweet['_id'])
        accounts_ids = {name:tweet_ids}
    else:
        tweet_ids = accounts_ids.get(name)
        tweet_ids.append(tweet['_id'])
        del accounts_ids[name]
        accounts_ids = {name:tweet_ids}

for item in accounts_ids:
    print item

'''for tweet in tweets:
    accounts.append(tweet['user']['screen_name'])
    #print tweet['user']['screen_name']

account_names = Set(accounts)

#print account_names

try:
    for name in account_names:
        key = 'user.{screen_name}'.format(screen_name=name)
        #tweet_ids = db.tweets.find({"user.screen_name":name}).distinct("_id")
        tweet_ids= db.tweets.find({key: {'$exists': True}})
        for id in tweet_ids:
            ids.append(id['_id'])
        print str(ids)
        break
        numbers = []
        phones = db_parsed.phones.find({"id":{"$in":ids}})
        for p in phones:
            numbers.append(p['phone_no'])
        print name+"\t\t"+str(list(Set(numbers)))

except:
    print sys.exc_info()'''


