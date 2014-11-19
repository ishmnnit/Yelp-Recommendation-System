from __future__ import division
from pymongo import MongoClient
from settings import Settings
import operator

#Intialize all collection
userscore_collection = MongoClient(Settings.MONGO_CONNECTION_STRING)[Settings.TOPICS_DATABASE][Settings.USERSCORE_COLLECTION]
businessscore_collection = MongoClient(Settings.MONGO_CONNECTION_STRING)[Settings.TOPICS_DATABASE][Settings.BUSINESSSCORE_COLLECTION]
reco_collection = MongoClient(Settings.MONGO_CONNECTION_STRING)[Settings.TOPICS_DATABASE][Settings.RECOMMENDATION_COLLECTION]


# Go through each user and compute top 20 Business for each User 
print userscore_collection.count()
print businessscore_collection.count()
userScoreCollection = userscore_collection.find()


bulk = reco_collection.initialize_unordered_bulk_op()
counter=0
bulkCounter = 0

for user in userScoreCollection:
    
    userTopics = user["userscore"]
    #print "length of user topics " + str(len(userTopics))
    
    ratings= { }
    businessScoreCollection = businessscore_collection.find()
    for business in businessScoreCollection:
        businessTopics = business["businessscore"]
        #print "length of Business topics " + str(len(businessTopics))
        #print "busines=" + str(business["business"])
        businessRatings = 0.0
        for i in range(1,len(userTopics)):
            businessRatings = businessRatings + float(userTopics[i])*float(businessTopics[i])
        
        #print "Business Ratings=" + str(businessRatings)
        ratings[business["business"]]=businessRatings
    sorted_x = sorted(ratings.items(), key=operator.itemgetter(1),reverse = True)
    
    recoList=sorted_x
 
   
    

    counter = counter + 1
    bulkCounter = bulkCounter + 1
    bulk.insert({"userId":user["userId"],"recoList":recoList})
    if bulkCounter % 1000 == 0 :
        bulk.execute()
        bulk = reco_collection.initialize_unordered_bulk_op()
        bulkCounter=0

##
if bulkCounter > 0 :
    bulk.execute()
    




