__author__ = 'anouksha'

import pymongo
from sets import Set
import sys

db = pymongo.MongoClient().Twitter
db_parsed = pymongo.MongoClient().TwitterParsed

tweets = db.tweets.find().limit(200)
accounts = []

for tweet in tweets:
    accounts.append(tweet['user']['screen_name'])
    #print tweet['user']['screen_name']

account_names = Set(accounts)

#print account_names

try:
    for name in account_names:
        key = 'user.{screen_name}'.format(screen_name=name)
        #tweet_ids = db.tweets.find({"user.screen_name":name}).distinct("_id")
        tweet_ids = db.tweets.find({key: {'$exists': True}}).distinct("_id")
        numbers = []
        phones = db_parsed.phones.find({"id":{"$in":tweet_ids}})
        for p in phones:
            numbers.append(p['phone_no'])
        print name+"\t\t"+str(list(Set(numbers)))

except:
    print sys.exec_info()


