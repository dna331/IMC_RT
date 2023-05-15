from confluent_kafka import Consumer, KafkaError
import json
import pymongo

# Configure the Kafka consumer
consumer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'mygroup',
    'auto.offset.reset': 'earliest'
}

client = pymongo.MongoClient("mongodb+srv://quanha:Quanpro502799@cluster0.pc0sjuq.mongodb.net/?retryWrites=true&w=majority",connectTimeoutMS=None, socketTimeoutMS=None)
db = client.imagecaption
collection = db['source_data']

# Create a Kafka consumer instance
consumer = Consumer(consumer_conf)

# Define the Kafka topic to consume from
topic = 'caption_data'

# Subscribe to the Kafka topic
consumer.subscribe([topic])

# Continuously poll for new messages
while True:
    message = consumer.poll(1.0)
    count = 0
    # Check for any errors
    if message is None:
        continue
    if message.error():
        if message.error().code() == KafkaError._PARTITION_EOF:
            print('Reached end of partition')
        else:
            print(f'Error while consuming message: {message.error()}')
        continue

    # Decode the message payload from bytes to string
    payload = message.value().decode('utf-8')

    # Parse the message payload as JSON
    try:
        data = json.loads(payload)
    except json.JSONDecodeError as e:
        print(f'Error while decoding JSON payload: {e}')
        continue
    # Update the MongoDB collection with the new data
    new_image = {
        "license": 0,
        "file_name": data['id']+'.jpg',
        "coco_url": data['image_url'],
        "height": data['width'],
        "width": data['height'],
        "date_captured": "",
        "flickr_url": "",
        "id": data['id'] # Extract the image_id from the image file name
    }

    new_annotation = {
        "image_id": new_image["id"],
        "caption": data['caption'],
        "id": int(9000000) + count # Generate a new id for the annotation
    }

    new_is_new = {
        "image_id": new_image["id"],
        "is_new": True
    }
    # result = collection.update_one({'id': data['id']}, {'$set': {'images': data['images'], 'annotations': data['annotations'], 'is_new': True}}, upsert=True)
    collection.update_one({}, {"$push": {"images": new_image}})
    collection.update_one({}, {"$push": {"annotations": new_annotation}})
    collection.update_one({}, {"$push": {"is_new": new_is_new}})
    # Process the message
    result = collection.find_one({'images.id': new_image["id"]})
    count+=1
    print(f'stored sample to db: {data}')
