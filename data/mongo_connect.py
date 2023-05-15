import pymongo
import json

# Load the JSON data from file
with open('./data/uitviic_captions_train2017.json', 'r') as f:
    data = json.load(f)

# Connect to the local MongoDB instance
client = pymongo.MongoClient('mongodb://localhost:27017/')

# Create a new database and collection
db = client['imagecaption']
collection = db['source_data']

# Insert the JSON data into the collection
collection.insert_one(data)