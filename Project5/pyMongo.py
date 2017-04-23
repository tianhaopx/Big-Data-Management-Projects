from pymongo import MongoClient, IndexModel, TEXT
import json
import subprocess
from bson import json_util
from bson.code import Code

# creating the link
client = MongoClient('localhost', 27017)
db = client.project5
db.tree.drop()
collection = db.tree
print("------------------------")
print("Question 1")
print("------------------------")
print("Load the Q1 example data")
with open("Q1_pyMongo_data.json") as f:
    data = f.read()

data = json_util.loads(data)
collection.insert(data)
print(db.tree.count())
print("========\t")

# 1) Assume we model the records and relationships in Figure 1 using the Parent-Referencing model (Slide 49 in
# MongoDB-2). Write a query to report the ancestors of “MongoDB”. The output should be an array containing
# values [{Name: “Databases”, Level: 1},
#         {Name: “Programming”, Level: 2},
#         {Name: “Books”, Level: 3}]
print("Question 1.1")
node = collection.find_one({"_id": "MongoDB"})
queue = [node["parent"]]
lvl = 0
while queue:
    temp = queue.pop(0)
    ans = collection.find_one({"_id": temp})
    if "parent" in ans:
        if ans["parent"] != None:
            queue.append(ans["parent"])
    lvl += 1
    print({"name": ans["_id"], "Level": lvl})
print("===============")

# 2) Assume we model the records and relationships in Figure 1 using the Parent-Referencing model (Slide 49 in
# MongoDB-2). You are given only the root node, i.e., _id = “Books”, write a query that reports the height of the tree.
# (It should be 4 in our case)
print("Question 1.2")
root = collection.find_one({"_id": "Books"})
queue = [[root]]
height = 0
while queue:
    height += 1
    node = queue.pop(0)
    temp = []
    for n in node:
        child = collection.find({"parent": n["_id"]})
        if child:
            for n in child:
                temp.append(n)
    if temp != []:
        queue.append(temp)
print("The height of the tree is: ", height)
print("===============")

# 3) Assume we model the records and relationships in Figure 1 using the Child-Referencing model (Slide 54 in
# MongoDB-2). Write a query to report the parent of “dbm”.
print("Question 1.3")
node = collection.find_one({"_id": "dbm"})
queue = [node]
parent = []
while queue:
    temp = queue.pop(0)
    ans = collection.find_one({'children': temp["_id"]})
    if ans:
        parent.append(ans["_id"])
        queue.append(ans)
print("The parent of dbm is", parent)
print("===============")

# 4) Assume we model the records and relationships in Figure 1 using the Child-Referencing model (Slide 54 in
# MongoDB-2). Write a query to report the descendants of “Books”. The output should be an array containing values
# [“Programming”, “Languages”, “Databases”, “MongoDB”, “dbm”]
print("Question 1.4")
node = collection.find_one({"_id": "Books"})
queue = [node]
descendants = []
while queue:
    temp = queue.pop(0)
    ans = temp["children"]
    if ans:
        for n in ans:
            descendants.append(n)
            queue.append(collection.find_one({"_id": n}))
print("The descendants of books is", descendants)
print("===============")

# 5) Assume we model the records and relationships in Figure 1 using the Child-Referencing model (Slide 54 in
# MongoDB-2). Write a query to report the siblings “Databases”.
print("Question 1.5")
node = collection.find({"children": "Databases"})
siblings = []
for n in node:
    for child in n["children"]:
        if child != "Databases":
            siblings.append(child)
print("The siblings of Databases is", siblings)
print("------------------------")
print("Question 2")
print("------------------------")

db = client.project5
db.bios.drop()
collection = db.bios
print("Load the Q2 example data")
with open("Q2_pyMongo_data.json") as f:
    data = f.read()

data = json_util.loads(data)
collection.insert(data)
print(db.bios.count())
print("========\t")
# 1) Write an aggregation query that groups by the award name, i.e., the “award” field inside the “awards”
# array, and reports the count of each award. (Use Map-Reduce mechanism)
print("Question 2.1")
mapper = Code("""
                function(){
                    if (this.awards){
                        for (var idx = 0; idx<this.awards.length; idx++){
                            var key = this.awards[idx].award;
                            var value = 1;
                            emit(key, value);
                        }
                    }
                }
              """)

reducer = Code("""
                function(key, values){
                var total = 0;
                for (var idx = 0; idx<values.length; idx++) { 
                    total += values[idx];
                    }
                return total;
                }
              """)
result = collection.map_reduce(mapper, reducer, "myresults")
for n in result.find():
    print(n)
print("========\t")
# 2) Write an aggregation query that groups by the birth year, i.e., the year within the “birth” field, are
# report an array of _ids for each birth year. (Use Aggregate mechanism)
print("Question 2.2")
ans = collection.aggregate([
    {"$match": {"birth": {"$exists": True}}},
    {"$project": {"year": {"$year": "$birth"}, "_id": "$name"}}
])
for n in ans:
    print(n)
print("========\t")
# 3) Report the document with the smallest and largest _ids. You first need to find the values of the smallest
# and largest, and then report their documents.

print("Question 2.3")
ans = collection.aggregate([
    {"$sort": {"_id": -1}},
    {"$limit": 1}
])
print("Largest")
for n in ans:
    print(n)
ans = collection.aggregate([
    {"$sort": {"_id": 1}},
    {"$limit": 1}
])
print("Smallest")
for n in ans:
    print(n)
print("========\t")

# 4) Use the $text operator to search for and report all documents containing “Turing Award” as one
# sentence (not separate keywords).
print("Question 2.4")
idx = IndexModel([("awards.award",TEXT)])
collection.create_indexes([idx])
ans = collection.find({"$text": {"$search": "\"Turing Award\""}})
for n in ans:
    print(n)
print("========\t")
# 5) Use the $text operator to search for and report all documents containing either “Turing” or “National
# Medal”.
print("Question 2.5")
idx = IndexModel([("awards.award",TEXT)])
collection.create_indexes([idx])
ans = collection.find({"$text": {"$search": "\"Turing Award\""}})
id = [n for n in ans]
ans = collection.find({"$text": {"$search": "\"National Medal\" -\"Turing Award\""}})
for n in ans:
    id.append(n)
for n in id:
    print(n)
print("========\t")
