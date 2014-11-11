import json
import sys
sys.path.insert(0, '/home/ish/DataScience/yelp_topic')


from pymongo import MongoClient
from settings import Settings


dataset_file = Settings.DATASET_FILE
reviews_collection = MongoClient(Settings.MONGO_CONNECTION_STRING)[Settings.REVIEWS_DATABASE][
    Settings.REVIEWS_COLLECTION]

# Bulk insertion is used to make insertion faster
bulk= reviews_collection.initialize_unordered_bulk_op()
bulkCounter=0
counter=0

#Read each line and insert data in MongoDB in Json format
with open(dataset_file) as dataset:
    next(dataset)
    for line in dataset:
        try:
            data = json.loads(line)
        except ValueError:
            print 'Oops!'
        if data["type"] == "review":
            bulk.insert({
                "userId":data["user_id"],
                "rating":data["stars"],
                "reviewId": data["review_id"],
                "business": data["business_id"],
                "text": data["text"]
            })

        bulkCounter = bulkCounter + 1
        counter = counter + 1
        if bulkCounter % 10000 == 0:
            bulk.execute()
            print str(counter) + ' data inserted in Reviews collection of Dataset_Challenge_Reviews '
            bulk= reviews_collection.initialize_unordered_bulk_op()
            bulkCounter=0

if bulkCounter > 0 :
    bulk.execute()

