from pymongo import MongoClient
import json
import subprocess
from bson import json_util

# creating the link
client = MongoClient('localhost', 27017)
db = client.project4
db.bios.drop()
collection = db.bios

# using mongoimport to import the data
# subprocess.call(["mongoimport", "--db", "project4", "--collection", "bios", "--drop", "--file", "/home/test/IdeaProjects/BigDataProjects/Project4/data_shell.json", "--jsonArray"])

# you need to do somethind before you insert the data
# 1) Insert the data
with open("data_pymongo.json") as f:
    data = f.read()

data = json_util.loads(data)
collection.insert(data)
print(db.bios.count())

# 2) Write a CRUD operation(s) that changes the _id of “John McCarthy” to value 2.
print(db.bios.find_one({"name.first": "John", "name.last": "McCarthy"}))
temp = db.bios.find_one({"name.first": "John", "name.last": "McCarthy"})
temp['_id'] = 2
del temp['oid']
db.bios.delete_many({"name.first": "John", "name.last": "McCarthy"})
db.bios.insert(temp)
print(db.bios.find_one({"name.first": "John", "name.last": "McCarthy"}))

# 3) Write a CRUD operation(s) that inserts the following new records into the collection:
with open("new_data.json") as f:
    data = f.read()
data = json_util.loads(data)
collection.insert(data)
print(db.bios.count())

# 4) Report all documents of people who got a “Turing Award” after 1976
ans = db.bios.find({"awards": {"$elemMatch": {"award": "Turing Award", "year": {"$gt": 1976}}}})
for n in ans:
    print(n)

# 5) Report all documents of people who got less than 3 awards or have contribution in “FP”
# for n in db.bios.aggregate([{"$group":{"_id":"$_id","total":{"$sum":{"$size":{"$ifNull":["$awards",[]]}}}}}]):
ans = db.bios.find(
    {"$or":
        [
            {"contribs": "FP"},
            {"awards.2": {"$exists": False}}
        ]})
for n in ans:
    print(n)

# 6) Report the contributions of “Dennis Ritchie” (only report the name and the contribution array)
ans = db.bios.find_one({"name.first": "Dennis", "name.last": "Ritchie"}, {'name': 1, "contribs": 1, "_id": 0})
print(ans)

# 7) Update the document of “Guido van Rossum” to add “OOP” to the contribution list.
print(db.bios.find_one({"name.first": "Guido", "name.last": "van Rossum"}))
db.bios.update_one({"name.first": "Guido", "name.last": "van Rossum"}, {"$push": {"contribs": "OOP"}})
print(db.bios.find_one({"name.first": "Guido", "name.last": "van Rossum"}))

# 8) Insert a new filed of type array, called “comments”, into the document of “Alex Chen” storing the
#    following comments: “He taught in 3 universities”, “died from cancer”, “lived in CA”
print(db.bios.find_one({"name.first": "Alex", "name.last": "Chen"}))
db.bios.update_one({"name.first": "Alex", "name.last": "Chen"},
                   {"$push": {
                       "comments": {"$each": ["He taught in 3 universities", "died from cancer", "lived in CA"]}}})
print(db.bios.find_one({"name.first": "Alex", "name.last": "Chen"}))

# 9) For each contribution by “Alex Chen”, say X, list the peoples’ names (fisrt and last) who have
#    contribution X. E.g., Alex Chen has two contributions in “C++” and “Simula”. Then, the output
#    should be similar to:
#    a. {Contribution: “C++”,
#    People: [{first: “Alex”, last: “Chen”}, {first: “David”, last: “Mark”}]},
#    { Contribution: “Simula”,
#    ....}
contributions = db.bios.find_one({"name.first": "Alex", "name.last": "Chen"}, {"contribs": 1, '_id': 0})['contribs']
temp = db.bios.aggregate([
    {"$unwind": "$contribs"},
    {"$group": {"_id": "$contribs", "people": {"$addToSet": "$name"}}},
    {"$match": {"_id": {"$in": contributions}}}
])
for n in temp:
    print(n)

# 10) Report all documents where the first name matches the regular expression “Jo*”, where “*” means any
#     number of characters. Report the documents sorted by the last name.
ans = db.bios.find({"name.first": {"$regex": "^Jo"}})
for n in ans:
    print(n)

# 11) Report the distinct organization that gave awards. This information can be found in the “by” field
#     inside the “awards” array. The output should be an array of the distinct values, e.g., [“wpi’, “acm’, ...]
# db.bios.aggregate([
#     {"$unwind": "$awards"},
#     {"$group": {"_id": "$awards.by"}}
# ])
db.bios.distinct("awards.by")

# 12) Delete from all documents the “death” field.
db.bios.update({}, {"$unset": {"death": {}}}, multi=True)

# 13) Delete from all documents any award given on 2011.
db.bios.update({}, {"$pull": {"awards": {"year": {"$in": [2011,'2011']}}}}, multi=True)

# 14) Update the award of document _id =30, which is given by WPI, and set the year to 1965.
print(db.bios.find_one({"_id": 30}))
db.bios.update({"_id": 30, "awards.by": "WPI"}, {"$set": {"awards.$.year": 1965}})
print(db.bios.find_one({"_id": 30}))

# 15) Add (copy) all the contributions of document _id = 3 to that of document _id = 30
temp = db.bios.find_one({"_id": 3})['contribs']
print(db.bios.find_one({"_id": 30}))
for n in temp:
    db.bios.update({"_id": 30}, {"$push": {"contribs": n}})
print(db.bios.find_one({"_id": 30}))

# 16) Report only the names (first and last) of those individuals who won at least two awards in 2001.
ans = db.bios.aggregate([
    {"$unwind": "$awards"},
    {"$match": {"awards.year": 2001}},
    {"$group": {"_id": "$name", "awards": {"$sum": 1}}},
    {"$match": {"awards": {"$gt": 1}}},
    {"$group": {"_id": "$_id"}}
])
for n in ans:
    print(n)

# 17) Report the document with the largest id. First, you need to find the largest _id (using a CRUD
#     statement), and then use that to report the corresponding document.
temp = db.bios.find_one({"$query": {}, "$orderby": {"_id": -1}})

# 18) Report only one document where one of the awards is given by “ACM”.
ans = db.bios.find_one({"awards.by": "ACM"})

# 19) Delete the documents inserted in Q3, i.e., _id = 20 and 30.
print(db.bios.count())
db.bios.delete_many({"_id": {"$in": [20, 30]}})
print(db.bios.count())

# 20) Report the number of documents in the collection.
print(db.bios.count())
