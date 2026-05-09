from pymongo import MongoClient
from config import MONGO_URI, DB_NAME

client = MongoClient(MONGO_URI)

print("Databases before drop:", client.list_database_names())
client.drop_database(DB_NAME)
print(f"Dropped database: {DB_NAME}")
print("Databases after drop:", client.list_database_names())

client.close()