__author__ = 'anouksha'

import pymongo
import string

db = pymongo.MongoClient().Twitter
tweets = db.tweets.find()

for tweet in tweets:
    text = tweet['text']
    #print text
    new_text=text

    if tweet['entities']['user_mentions']:
        for u in tweet['entities']['user_mentions']:
            start = u['indices'].pop(0)
            end = u['indices'].pop(0)
            mention = text[start:end]
            new_text = new_text.replace(mention,"")

    if tweet['entities']['urls']:
        for url in tweet['entities']['urls']:
            start = url['indices'].pop(0)
            end = url['indices'].pop(0)
            u = text[start:end]
            new_text = new_text.replace(u,"")

    print new_text

    t={}
    t['tweet_id']=tweet['_id']
    t['text']=new_text

    db.processed_tweets.insert(t)
    #print "---------------------------------------------------------------------------------"
