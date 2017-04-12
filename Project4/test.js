// mongo test.js >> out.txt
// use test
db = db.getSiblingDB('test')
db.test.drop()
// 1) Create a collection named “test”, and insert into this collection the documents found in this link (10 documents): http://docs.mongodb.org/manual/reference/bios-example-collection/
print("#1")
db.test.insertMany([{"_id":1,"name":{"first":"John","last":"Backus"},"birth":ISODate("1924-12-03T05:00:00Z"),"death":ISODate("2007-03-17T04:00:00Z"),"contribs":["Fortran","ALGOL","Backus-Naur Form","FP"],"awards":[{"award":"W.W. McDowell Award","year":1967,"by":"IEEE Computer Society"},{"award":"National Medal of Science","year":1975,"by":"National Science Foundation"},{"award":"Turing Award","year":1977,"by":"ACM"},{"award":"Draper Prize","year":1993,"by":"National Academy of Engineering"}]},{"_id":ObjectId("51df07b094c6acd67e492f41"),"name":{"first":"John","last":"McCarthy"},"birth":ISODate("1927-09-04T04:00:00Z"),"death":ISODate("2011-12-24T05:00:00Z"),"contribs":["Lisp","Artificial Intelligence","ALGOL"],"awards":[{"award":"Turing Award","year":1971,"by":"ACM"},{"award":"Kyoto Prize","year":1988,"by":"Inamori Foundation"},{"award":"National Medal of Science","year":1990,"by":"National Science Foundation"}]},{"_id":3,"name":{"first":"Grace","last":"Hopper"},"title":"Rear Admiral","birth":ISODate("1906-12-09T05:00:00Z"),"death":ISODate("1992-01-01T05:00:00Z"),"contribs":["UNIVAC","compiler","FLOW-MATIC","COBOL"],"awards":[{"award":"Computer Sciences Man of the Year","year":1969,"by":"Data Processing Management Association"},{"award":"Distinguished Fellow","year":1973,"by":" British Computer Society"},{"award":"W. W. McDowell Award","year":1976,"by":"IEEE Computer Society"},{"award":"National Medal of Technology","year":1991,"by":"United States"}]},{"_id":4,"name":{"first":"Kristen","last":"Nygaard"},"birth":ISODate("1926-08-27T04:00:00Z"),"death":ISODate("2002-08-10T04:00:00Z"),"contribs":["OOP","Simula"],"awards":[{"award":"Rosing Prize","year":1999,"by":"Norwegian Data Association"},{"award":"Turing Award","year":2001,"by":"ACM"},{"award":"IEEE John von Neumann Medal","year":2001,"by":"IEEE"}]},{"_id":5,"name":{"first":"Ole-Johan","last":"Dahl"},"birth":ISODate("1931-10-12T04:00:00Z"),"death":ISODate("2002-06-29T04:00:00Z"),"contribs":["OOP","Simula"],"awards":[{"award":"Rosing Prize","year":1999,"by":"Norwegian Data Association"},{"award":"Turing Award","year":2001,"by":"ACM"},{"award":"IEEE John von Neumann Medal","year":2001,"by":"IEEE"}]},{"_id":6,"name":{"first":"Guido","last":"van Rossum"},"birth":ISODate("1956-01-31T05:00:00Z"),"contribs":["Python"],"awards":[{"award":"Award for the Advancement of Free Software","year":2001,"by":"Free Software Foundation"},{"award":"NLUUG Award","year":2003,"by":"NLUUG"}]},{"_id":ObjectId("51e062189c6ae665454e301d"),"name":{"first":"Dennis","last":"Ritchie"},"birth":ISODate("1941-09-09T04:00:00Z"),"death":ISODate("2011-10-12T04:00:00Z"),"contribs":["UNIX","C"],"awards":[{"award":"Turing Award","year":1983,"by":"ACM"},{"award":"National Medal of Technology","year":1998,"by":"United States"},{"award":"Japan Prize","year":2011,"by":"The Japan Prize Foundation"}]},{"_id":8,"name":{"first":"Yukihiro","aka":"Matz","last":"Matsumoto"},"birth":ISODate("1965-04-14T04:00:00Z"),"contribs":["Ruby"],"awards":[{"award":"Award for the Advancement of Free Software","year":"2011","by":"Free Software Foundation"}]},{"_id":9,"name":{"first":"James","last":"Gosling"},"birth":ISODate("1955-05-19T04:00:00Z"),"contribs":["Java"],"awards":[{"award":"The Economist Innovation Award","year":2002,"by":"The Economist"},{"award":"Officer of the Order of Canada","year":2007,"by":"Canada"}]},{"_id":10,"name":{"first":"Martin","last":"Odersky"},"contribs":["Scala"]}])
print(db.test.count())
print("========")

// 2) Write a CRUD operation(s) that changes the _id of “John McCarthy” to value 2.
print("#2")
var doc = db.test.findOne({"name":{"first":"John","last":"McCarthy"}})
doc._id = 2
db.test.remove({"name":{"first":"John","last":"McCarthy"}})
db.test.insert(doc)
printjson(db.test.findOne({"name":{"first":"John","last":"McCarthy"}}))
print("========")

// 3) Write a CRUD operation(s) that inserts the following new records into the collection:
print("#3")
db.test.insertMany([{"_id":20,"name":{"first":"Alex","last":"Chen"},"birth":ISODate("1933-08-27T04:00:00Z"),"death":ISODate("1984-11-07T04:00:00Z"),"contribs":["C++","Simula"],"awards":[{"award":"WPI Award","year":1977,"by":"WPI"}]},{"_id":30,"name":{"first":"David","last":"Mark"},"birth":ISODate("1911-04-12T04:00:00Z"),"death":ISODate("2000-11-07T04:00:00Z"),"contribs":["C++","FP","Lisp"],"awards":[{"award":"WPI Award","year":1963,"by":"WPI"},{"award":"Turing Award","year":1966,"by":"ACM"}]}])
print(db.test.count())
print("========")

// 4) Report all documents of people who got a “Turing Award” after 1976
print("#4")
db.test.find({awards:{"$elemMatch":{award:"Turing Award",year:{$gt:1976}}}}).forEach(function(doc){printjson(doc)})
print("========")

// 5) Report all documents of people who got less than 3 awards or have contribution in “FP”
print("#5")
db.test.find({$or:[{awards:{$exists:false}},{awards:{$size:0}},{awards:{$size:1}},{awards:{$size:2}},{contribs:"FP"}]}).forEach(function(doc){printjson(doc)})
print("========")

// 6) Report the contributions of “Dennis Ritchie” (only report the name and the contribution array)
print("#6")
db.test.find({"name":{"first":"Dennis","last":"Ritchie"}},{name:1,contribs:1,_id:0}).forEach(function(doc){printjson(doc)})
print("========")

// 7) Update the document of “Guido van Rossum” to add “OOP” to the contribution list.
print("#7")
db.test.update({"name":{"first":"Guido","last":"van Rossum"}},{$push:{contribs:"OOP"}})
printjson(db.test.findOne({"name":{"first":"Guido","last":"van Rossum"}}))
print("========")

// 8) Insert a new filed of type array, called “comments”, into the document of “Alex Chen” storing the following comments: “He taught in 3 universities”, “died from cancer”, “lived in CA”
print("#8")
db.test.update({"name":{"first":"Alex","last":"Chen"}},{$push:{comments:{$each:["He taught in 3 universities","died from cancer","lived in CA"]}}})
printjson(db.test.findOne({"name":{"first":"Alex","last":"Chen"}}))
print("========")

// 9) For each contribution by “Alex Chen”, say X, list the peoples’ names (fisrt and last) who have contribution X.
print("#9")
var array = db.test.find({"name":{"first":"Alex","last":"Chen"}},{contribs:1,_id:0}).toArray()[0].contribs
db.test.aggregate([{$unwind:"$contribs"},{$match:{contribs:{$in:array}}},{$group:{_id:"$contribs",people:{$addToSet:"$name"}}}]).forEach(function(doc){printjson(doc)})
print("========")

// 10) Report all documents where the first name matches the regular expression “Jo*”, where “*” means any number of characters. Report the documents sorted by the last name.
print("#10")
db.test.find({"name.first":{$regex:/^Jo/}}).sort({"name.last":1}).forEach(function(doc){printjson(doc)})
print("========")

// 11) Report the distinct organization that gave awards. This information can be found in the “by” field inside the “awards” array.
print("#11")
db.test.distinct("awards.by").forEach(function(doc){printjson(doc)})
print("========")

// 12) Delete from all documents the “death” field.
print("#12")
db.test.updateMany({},{$unset:{death:""}})
print(db.test.count())
print("========")

// 13) Delete from all documents any award given on 2011. Note: year in _id:8 was String
print("#13")
db.test.updateMany({},{$pull:{awards:{year:{$in:[2011, "2011"]}}}})
print(db.test.count())
print("========")

// 14) Update the award of document _id =30, which is given by WPI, and set the year to 1965.
print("#14")
db.test.update({_id:30,"awards.by":"WPI"},{$set:{"awards.$.year":1965}})
printjson(db.test.findOne({_id:30}))
print("========")

// 15) Add (copy) all the contributions of document _id = 3 to that of document _id = 30
print("#15")
db.test.find({_id:3},{contribs:1,_id:0}).forEach(function (doc) {
  db.test.update({_id:30},{$push:{contribs:{$each:doc.contribs}}})
})
printjson(db.test.findOne({_id:30}))
print("========")

// 16) Report only the names (first and last) of those individuals who won at least two awards in 2001.
print("#16")
db.test.aggregate([{$unwind:"$awards"},{$match:{"awards.year":2001}},{$group:{_id:"$name",num:{$sum:1}}},{$match:{num:{$gte:2}}},{$project:{num:0}}]).forEach(function(doc){printjson(doc)})
print("========")

// 17) Report the document with the largest id.
print("#17")
db.test.find().sort({_id:-1}).limit(1).forEach(function(doc){printjson(doc)})
print("========")

// 18) Report only one document where one of the awards is given by “ACM”.
print("#18")
printjson(db.test.findOne({"awards.by":"ACM"}))
print("========")

// 19) Delete the documents inserted in Q3, i.e., _id = 20 and 30.
print("#19")
db.test.remove({$or:[{_id:20},{_id:30}]})
print(db.test.count())
print("========")

// 20) Report the number of documents in the collection.
print("#20")
print(db.test.count())
print("========")
