from pymongo import MongoClient

MONGO_URI = "mongodb://localhost:27117/"

client = MongoClient(MONGO_URI)
db = client["ais"]
collection = db["test"]

collection.insert_one({"msg": "hello mongo"})
print(collection.find_one({"msg": "hello mongo"}))