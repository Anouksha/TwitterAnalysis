import pymongo

db = pymongo.MongoClient().TwitterParsed
numbers = db.phones.distinct("phone_no")
nc = 0

for n in numbers:
	nc += 1

print "No. of unique phone numbers: "+str(nc)
