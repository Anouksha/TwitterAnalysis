__author__ = 'anouksha'

import pymongo
import re
#import numpy
import time
import math

db_parsed = pymongo.MongoClient().TwitterParsed
db = pymongo.MongoClient().tweets
numbers = db.bharat_phone_stats.distinct("number")
#phone_numbers = db_parsed.bharat_phonestrain.aggregate([{"$group":{"_id":"$phone_no"}}])
#phone_tweets = db_parsed.bharat_phonestrain.find()
phone_numbers = []
for num in numbers:
    p_no = ''
    for n in num:
        if n.isdigit():
            p_no+=n
    if p_no not in phone_numbers:
        phone_numbers.append(p_no)
#zeroPattern = re.compile('^0')


def get_phone_count(tweets):
    return tweets.count()

def get_unique_representations(tweets):
    original = tweets.distinct("o_pno")
    total = len(original)
    return total

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

def get_std_dev_text(number, mean):
    calc = 0.0
    n = 0.0
    #print "Mean: "+str(mean)
    tweets = db_parsed.phonestrain.find({"phone_no":number})
    for tweet in tweets:
        diff = (len(tweet['text'])-mean)*1.0
        calc += math.pow(diff,2)
        #print calc
        n += 1.0
    val = (calc*1.0)/(n)
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

def get_num_sources(tweets):
    sources = tweets.distinct("source")
    return len(sources)

def get_num_truncated(number):
    count = db_parsed.phonestrain.find({"$and":[{"phone_no":number},{"truncated":True}]}).count()
    return count

def get_num_retweeted(number):
    count = db_parsed.phonestrain.find({"$and":[{"phone_no":number},{"retweeted":True}]}).count()
    return count

def get_num_verified(number):
    count = db_parsed.phonestrain.find({"$and":[{"phone_no":number},{"user.verified":True}]}).count()
    return count

def get_num_user_mentions(number):
    tweets = db_parsed.phonestrain.find({"$and":[{"phone_no":number},{"entities.user_mentions":{"$not": {"$size": 0}}}]})
    total_mentions = 0
    for tweet in tweets:
        mentions = tweet['entities']['user_mentions']
        total_mentions += len(mentions)
    return total_mentions

def get_num_in_replies(number):
    count = db_parsed.phonestrain.find({"$and":[{"phone_no":number},{"in_reply_to_screen_name":{"$ne": None}}]}).count()
    return count

def get_total_urls(tweets):
    '''tweet_ids = tweets.distinct("_id")
    count = db_parsed.resolve_urls.find({"tweet_id":{"$in":tweet_ids}}).count()
    return count'''
    total = 0
    for tweet in tweets:
        urls = tweet['url_check']
        if urls:
            total += len(urls)
    return total

def get_unique_urls(tweets):
    '''tweet_ids = tweets.distinct("_id")
    ids = db_parsed.resolve_urls.find({"tweet_id":{"$in":tweet_ids}}).distinct("f_url")
    return len(ids)'''
    count = 0
    unique_urls = []
    for tweet in tweets:
        urls = tweet['url_check']
        if urls:
            for url in urls:
                if url['f_url'] not in unique_urls:
                    unique_urls.append(url)
    return len(unique_urls)

def get_num_jumped_timezones(number, tweets):
    accounts = tweets.distinct("user.screen_name")
    count = 0
    for account in accounts:
        timezones = db_parsed.phonestrain.find({"$and":[{"phone_no":number},{"user.screen_name":account}]}).distinct("user.timezone")
        if len(timezones)>1:
            count+=1
    return count

def get_total_hashtags(number):
    tweets = db_parsed.phonestrain.find({"$and":[{"phone_no":number},{"entities.hashtags":{"$not": {"$size": 0}}}]})
    total_hashtags = 0
    for tweet in tweets:
        hashtags = tweet['entities']['hashtags']
        total_hashtags += len(hashtags)
    return total_hashtags

def get_unique_hashtags(number):
    tweets = db_parsed.phonestrain.find({"$and":[{"phone_no":number},{"entities.hashtags":{"$not": {"$size": 0}}}]})
    unique_hashtags = []
    for tweet in tweets:
        hashtags = tweet['entities']['hashtags']
        for hashtag in hashtags:
            if hashtag['text'] not in unique_hashtags:
                unique_hashtags.append(hashtag['text'])
    return len(unique_hashtags)

def insert_to_db(tweet_data):
    db_parsed.bharat_phone_features.insert(tweet_data)

print "Starting feature extraction"
c=0

for phone in phone_numbers:

    tweets = db_parsed.bharat_phonestrain.find({"phone_no":phone})
    data = []
    data['phone_no'] = str(phone)
    data['count'] = get_phone_count(tweets)
    data['num_representations'] = get_unique_representations(tweets)
    data['num_accounts'] = get_num_of_accounts(tweets)
    data['mean_text'] = get_mean_text(tweets)
    data['deviation_text'] = get_std_dev_text(phone, data['mean_text'])
    data['first_occurrence'] = get_first_occurrence(tweets)
    data['last_occurrence'] = get_last_occurrence(tweets)
    data['num_sources'] = get_num_sources(tweets)
    data['num_truncated'] = get_num_truncated(phone)
    data['num_in_retweets'] = get_num_retweeted(phone)
    data['verified_num'] = get_num_verified(phone)
    data['user_mentions'] = get_num_user_mentions(phone)
    data['num_replies'] = get_num_in_replies(phone)
    data['total_urls'] = get_total_urls(tweets)
    data['unique_urls'] = get_unique_urls(tweets)
    data['jumped_timezones'] = get_num_jumped_timezones(phone, tweets)
    data['total_hashtags'] = get_total_hashtags(phone)
    data['unique_hashtags'] = get_unique_hashtags(phone)

    print data
    break
    #insert_to_db(data)

print "Done with Feature Extraction"


