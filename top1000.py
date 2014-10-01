__author__ = 'anouksha'
import pymongo

db = pymongo.MongoClient().TwitterParsed

numbers = db.phonestrain.aggregate({"$group":{"_id":"$phone_no", "count":{"$sum":1}}},{"$sort":{"count":-1}}).limit(1000)

for number in numbers:
    print number

print "Done"
