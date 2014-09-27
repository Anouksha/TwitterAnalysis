__author__ = 'anouksha'

import pymongo
import re
#import numpy
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

def get_std_dev_text(tweets, mean):
    calc = 0.0
    n = 0.0
    print "Mean: "+str(mean)
    for tweet in tweets:
        diff = (len(tweet['text'])-mean)*1.0
        calc += math.pow(diff,2)
        n += 1.0
    val = calc/(n)
    return math.sqrt(val)

def get_first_occurrence(tweets):
    dates = tweets.distinct("created_at")
    dates.sort()
    #return time.strptime(dates[0],"%Y-%m-%dT%H:%M:%S")
    return dates[0].isoformat(' ')

def get_last_occurrence(tweets):
    dates = tweets.distinct("created_at")
    dates.sort()
    #return time.strptime(dates[len(dates)-1], "%Y-%m-%dT%H:%M:%S")
    return dates[len(dates)-1].isoformat(' ')

def get_num_truncated(number):
    count = db_parsed.phonestrain.find({"$and":[{"phone_no":number},{"truncated":True}]}).count()
    return count

def get_num_retweeted(number):
    count = db_parsed.phonestrain.find({"$and":[{"phone_no":number},{"retweeted":True}]}).count()
    return count

def get_num_verified(number):
    count = db_parsed.phonestrain.find({"$and":[{"phone_no":number},{"user.verified":True}]}).count()
    return count

def get_num_jumped_timezones(number, tweets):
    accounts = tweets.distinct("user.screen_name")
    count = 0
    for account in accounts:
        timezones = db_parsed.phonestrain.find({"$and":[{"phone_no":number},{"user.screen_name":account}]}).distinct("user.timezone")
        if len(timezones)>1:
            count+=1
    return count

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
    data['deviation_text'] = get_std_dev_text(tweets, data['mean_text'])
    data['first_occurrence'] = get_first_occurrence(tweets)
    data['last_occurrence'] = get_last_occurrence(tweets)
    data['num_truncated'] = get_num_truncated(phone)
    data['num_in_retweets'] = get_num_retweeted(phone)
    data['verified_num'] = get_num_verified(phone)
    data['jumped_timezones'] = get_num_jumped_timezones(phone, tweets)
    c+=1
    print data
    if c==15:
        break


