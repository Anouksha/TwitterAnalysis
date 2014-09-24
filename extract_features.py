__author__ = 'anouksha'

import pymongo
import re

db_parsed = pymongo.MongoClient().TwitterParsed
phone_numbers = db_parsed.phonestrain.distinct("phone_no")
zeroPattern = re.compile('^0')


def getPhoneCount(tweets):
    return tweets.count()

def getUniqueRepresentations(tweets):
    original = tweets.distinct("o_pno")
    return original

def getNumOfAccounts(tweets):
    accounts = tweets.dictinct("user.screen_name")
    return accounts.size()

print "Starting feature extraction"
c=0

#print phone_numbers

for phone in phone_numbers:
    if zeroPattern.match(phone):
        continue
    tweets = db_parsed.phonestrain.find({"phone_no":phone})
    data = {}
    data['phone_no'] = phone
    data['count'] = getPhoneCount(tweets)
    data['representations'] = getUniqueRepresentations(tweets)
    data['num_accounts'] = getNumOfAccounts(tweets)
    c+=1
    print data
    if c==15:
        break


