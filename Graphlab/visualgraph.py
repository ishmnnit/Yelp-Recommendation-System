from pymongo import MongoClient
from settings import Settings
import operator
import csv

usergraph_collection = MongoClient(Settings.MONGO_CONNECTION_STRING)[Settings.TOPICS_DATABASE][Settings.USERGRAPH_COLLECTION]

#print usergraph_collection.count()

user_reco = usergraph_collection.find()

business_dict = {}
counter = 0
for user in user_reco:
    if user["business_id"] in business_dict:
        business_dict[user["business_id"]] += 1
        counter = counter + 1
    else:
        business_dict[user["business_id"]] = 1
print counter
business_list = sorted(business_dict.items(), key=operator.itemgetter(1), reverse=True)
writer = csv.writer(open('recoList.csv', 'wb'))
for key, value in business_list:
    writer.writerow([key, value])
