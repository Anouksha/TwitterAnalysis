__author__ = 'anouksha'

import pymongo
import re

db_parsed = pymongo.MongoClient().TwitterParsed
phone_numbers = db_parsed.phonestrain.distinct("phone_no")
#zeroPattern = re.compile(r'000000000*')


def getPhoneCount(tweets):
    return tweets.count()

def getUniqueRepresentations(tweets):
    original = tweets.distinct("o_pno")
    return original

print "Starting feature extraction"
c=0

for phone in phone_numbers:
    if phone.contains("00000000"):
        continue
    tweets = db_parsed.phonestrain.find({"phone_no":phone})
    data = {}
    data['phone_no'] = phone
    data['count'] = getPhoneCount(tweets)
    data['representations'] = getUniqueRepresentations(tweets)
    c+=1
    print data
    if c==5:
        break


