__author__ = 'anouksha'

import pymongo

db_parsed = pymongo.MongoClient().TwitterParsed

numbers = db_parsed.final_phone_features.find().sort([("count",pymongo.DESCENDING)]).limit(100)

for number in numbers:
    if(number['verified_num']>0):
        verified = "VERIFIED"
    else:
        verified = "NOT VERIFIED"
    print number['phone_no']+"\t"+str(number['count'])+"\t"+verified

print "Done"