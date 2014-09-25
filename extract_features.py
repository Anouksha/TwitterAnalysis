__author__ = 'anouksha'

import pymongo
import re
import numpy
import time
import math

db_parsed = pymongo.MongoClient().TwitterParsed
phone_numbers = db_parsed.phonestrain.distinct("phone_no")
zeroPattern = re.compile('^0')


def get_phone_count(tweets):
    return tweets.count()

def get_unique_representations(tweets):
    original = tweets.distinct("o_pno")
    return original

def get_num_of_accounts(tweets):
    accounts = tweets.distinct("user.screen_name")
    return len(accounts)

def get_mean_text(tweets):
    count = 0
    total_len = 0
    for tweet in tweets:
        count+=1
        total_len+=len(tweet['text'])
    return (total_len*1.0/count)

def get_std_dev_text(tweets):
    calc = 0
    n = 0
    tweets_2 = tweets
    mean = get_mean_text(tweets_2)
    for tweet in tweets:
        diff = math.abs(len(tweet['text'])-mean)
        calc += math.pow(diff,2)
        n+=1
    return math.sqrt(calc*1.0/n)

def get_first_occurrence(tweets):
    dates = tweets.distinct("created_at")
    dates.sort()
    return time.strptime(dates[0],"%Y-%m-%dT%H:%M:%S")

def get_last_occurrence(tweets):
    dates = tweets.distinct("created_at")
    dates.sort()
    return time.strptime(dates[len(dates)-1], "%Y-%m-%dT%H:%M:%S")

print "Starting feature extraction"
c=0

#print phone_numbers

for phone in phone_numbers:
    if zeroPattern.match(phone):
        continue
    tweets = db_parsed.phonestrain.find({"phone_no":phone})
    data = {}
    data['phone_no'] = phone
    data['count'] = get_phone_count(tweets)
    data['representations'] = get_unique_representations(tweets)
    data['num_accounts'] = get_num_of_accounts(tweets)
    data['mean_text'] = get_mean_text(tweets)
    data['deviation_text'] = get_std_dev_text(tweets)
    data['first_occurrence'] = get_first_occurrence(tweets)
    data['last_occurrence'] = get_last_occurrence(tweets)
    c+=1
    print data
    if c==15:
        break


