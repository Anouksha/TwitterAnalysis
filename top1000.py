__author__ = 'anouksha'
import pymongo
from bson.code import Code

db = pymongo.MongoClient().TwitterParsed

'''numbers = db.phonestrain.aggregate([{"$group":{"_id":"$phone_no", "count":{"$sum":1}}},{"$sort":{"count":-1}}]).limit(1000)

for number in numbers:
    print number

print "Done"'''

'''mapper = Code("""function () {
                var key = this.phone_no;
                var value = 1;
                emit(key,value);
             }
              """)
reducer = Code("""
              function (key, values) {
                var total = 0;
                  for (var i = 0; i < values.length; i++) {
                    total += values[i];
                  }
                  return total;
                }
                """)

result = db.phonestrain.map_reduce(mapper, reducer, "myresults")
for doc in result.find():
    print doc

print "Done"'''

def get_count(tweets):
    return tweets.count()

numbers = db.phonestrain.distinct("phone_no")

for number in numbers:
    tweets = db.phonestrain.find({"phone_no":number})
    count = get_count(tweets)
    print number + "\t" + str(count)

print "Done"



