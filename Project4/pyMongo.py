from pymongo import MongoClient
import json
import subprocess
from bson import json_util

# creating the link
client = MongoClient('localhost', 27017)
# getting the database
db = client.project4

# if collection exist we delete
db.bios.drop()
# getting a collection & import the data
collection = db.bios

# using mongoimport to import the data
# subprocess.call(["mongoimport", "--db", "project4", "--collection", "bios", "--drop", "--file", "/home/test/IdeaProjects/BigDataProjects/Project4/data_shell.json", "--jsonArray"])

# you need to do somethind before you insert the data
with open("data_pymongo.json") as f:
    data = f.read()

data = json_util.loads(data)
collection.insert(data)
print(db.bios.count())

# Write a CRUD operation(s) that changes the _id of “John McCarthy” to value 2.
print(db.bios.find_one({"name.first" : "John", "name.last" : "McCarthy" }))
temp = db.bios.find_one({"name.first" : "John", "name.last" : "McCarthy" })
temp['_id'] = 2
db.bios.delete_many({"name.first" : "John", "name.last" : "McCarthy" })
db.bios.insert(temp)
print(db.bios.find_one({"name.first" : "John", "name.last" : "McCarthy" }))

# Write a CRUD operation(s) that inserts the following new records into the collection:
with open("new_data.json") as f:
    data = f.read()
data = json_util.loads(data)
collection.insert(data)
print(db.bios.count())


# Report all documents of people who got a “Turing Award” after 1976
ans = db.bios.find({"awards":{"$elemMatch":{"award":"Turing Award","year":{"$gt":1976}}}})
for n in ans:
    print(n)

# Report all documents of people who got less than 3 awards or have contribution in “FP”
# for n in db.bios.aggregate([{"$group":{"_id":"$_id","total":{"$sum":{"$size":{"$ifNull":["$awards",[]]}}}}}]):
ans = db.bios.find(
                    {"$or":
                        [
                        {"contribs":"FP"},
                        {"where":"this.awards.length<3"}
                        ]})
