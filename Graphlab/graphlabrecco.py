import logging
import graphlab as gl
import multiprocessing
from pymongo import MongoClient
from settings import Settings

usergraph_collection = MongoClient(Settings.MONGO_CONNECTION_STRING)[Settings.TOPICS_DATABASE][Settings.USERGRAPH_COLLECTION]

#business = gl.SFrame.read_csv('yelp_academic_dataset_business.json', 
#                header=False, delimiter='\n', column_type_hints=dict)
#checkin = gl.SFrame.read_csv('yelp_academic_dataset_checkin.json', 
#                header=False, delimiter='\n', column_type_hints=dict)
review = gl.SFrame.read_csv('yelp_academic_dataset_review.json', 
                header=False, delimiter='\n', column_type_hints=dict)
#user = gl.SFrame.read_csv('yelp_academic_dataset_user.json', 
#                header=False, delimiter='\n', column_type_hints=dict)
#tip = gl.SFrame.read_csv('yelp_academic_dataset_tip.json', 
#                header=False, delimiter='\n', column_type_hints=dict)

# Changing JSON into tables, i.e. SFrames
reviews = review.unpack('X1', column_name_prefix='')
#businesses = business.unpack('X1', column_name_prefix='', 
#              limit=['business_id', 'name', 'stars'])

# Build a recommender system
m = gl.recommender.create(reviews, 'user_id', 'business_id', target='stars', ranking=True, verbose=True)
'''
# Find businesses that are similar based on users in common
similar_items = m.get_similar_items(['BVxlrYWgmi-8TPGMe6CTpg'])
print similar_items.join(businesses, on='business_id')
'''
#Recommend business to the users
recs = m.recommend()
#print recs[0]


#Bulk insertion
bulk = usergraph_collection.initialize_unordered_bulk_op()
bulkCounter=0
counter=0
for review in recs:
    bulk.insert({
        "user_id": review["user_id"],
        "score": review["score"],
        "business_id": review["business_id"]
        })
    bulkCounter = bulkCounter + 1
    if bulkCounter % 100000 == 0 :
        bulk.execute()
        print  str(counter)+ "Entries are inserted"
        bulkCounter=0
        counter=counter+1
        bulk = usergraph_collection.initialize_unordered_bulk_op()

if bulkCounter > 0:
    bulk.execute()
