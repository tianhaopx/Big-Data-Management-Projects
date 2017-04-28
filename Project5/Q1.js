//mongo Q1.js > Q1Result.txt

db = db.getSiblingDB("tree")
print("========")
print("Question 1.1")
db.tree.drop()
db.tree.insertMany([{ "_id": "MongoDB", "parent": "Databases" },{ "_id": "dbm", "parent": "Databases" },{ "_id": "Databases", "parent": "Programming" },{ "_id": "Languages", "parent": "Programming" },{ "_id": "Programming", "parent": "Books" },{ "_id": "Books", "parent": null }])
var stack = []
stack.push("MongoDB")
var level = 1
var result = []
while (stack.length > 0) {
  var current = stack.pop()
  var parent = db.tree.findOne({"_id": current}).parent
  if (parent != null) {
    stack.push(parent)
    var item = {}
    item.Name = parent
    item.Level = level
    level++
    result.push(item)
  }
}
printjson(result)
print("========")

print("Question 1.2")
var height = 1
var max_height = 1
var helper = function (item) {
  var children = db.tree.find({"parent": item})
  if (children.hasNext()) {
    height++
    while (children.hasNext()) {
      var child = children.next()._id
      helper(child)
    }
    height--
  } else {
    if (height > max_height) {
      max_height = height
    }
  }
  return max_height
}
var result = helper("Books")
print(result)
print("========")

print("Question 1.3")
db.tree.drop()
db.tree.insertMany([ { _id: "MongoDB", children: [] },{ _id: "dbm", children: [] },{ _id: "Databases", children: [ "MongoDB", "dbm" ] },{ _id: "Languages", children: [] },{ _id: "Programming", children: [ "Databases", "Languages" ] },{ _id: "Books", children: [ "Programming" ] }] )
var stack = []
var result = []
stack.push("dbm")
while (stack.length > 0) {
  var current = stack.pop()
  var parent = db.tree.findOne({"children": current})
  if (parent != null) {
    stack.push(parent._id)
    result.push(parent._id)
  }
}
printjson(result)
print("========")

print("Question 1.4")
var stack = []
var result = []
stack.push("Books")
while (stack.length > 0) {
  var current = stack.pop()
  var children = db.tree.findOne({"_id": current}).children
  if (children.length > 0) {
    for (var i in children) {
      var c = children[i]
      stack.push(c)
      result.push(c)
    }
  }
}
printjson(result)
print("========")

print("Question 1.5")
var result = db.tree.findOne({"children": "Databases"}).children
var i = result.indexOf("Databases")
result.splice(i, 1)
printjson(result)
