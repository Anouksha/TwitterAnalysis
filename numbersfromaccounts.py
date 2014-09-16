__author__ = 'anouksha'

import pymongo
from sets import Set

db = pymongo.MongoClient().Twitter
db_parsed = pymongo.MongoClient().TwitterParsed

tweets = db.tweets.find().limit(200)
accounts = []

for tweet in tweets:
    #accounts.append(tweet['user']['screen_name'])
    print tweet['user']['screen_name']

'''account_names = Set(accounts)

print account_names

for name in account_names:
    tweet_ids = db.tweets.find({"user.screen_name":name}).distinct("id")
    numbers = []
    phones = db_parsed.phones.find({"id":{"$in":tweet_ids}})
    for p in phones:
        numbers.append(p['phone_no'])'''
    #print name+"\t\t"+list(Set(numbers))


