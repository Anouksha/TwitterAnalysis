__author__ = 'anouksha'

import pymongo

db_parsed = pymongo.MongoClient().TwitterParsed
phone_numbers = db_parsed.phonestrain.distinct("phone_no")


def getPhoneCount(tweets):
    return tweets.count()

def getUniqueRepresentations(tweets):
    original = tweets.distinct("o_pno")
    return original

print "Starting feature extraction"

for phone in phone_numbers:
    tweets = db_parsed.phonestrain.find({"phone_no":phone})
    data = {}
    data['phone_no'] = phone
    data['count'] = getPhoneCount(tweets)
    data['representations'] = getUniqueRepresentations(tweets)
    break

print data

