from __future__ import division
import logging
import re
import multiprocessing
from gensim.models import LdaModel
from gensim import corpora
from pymongo import MongoClient
from settings import Settings
from sets import Set

# Intialize all collection
tags_collection = MongoClient(Settings.MONGO_CONNECTION_STRING)[Settings.TAGS_DATABASE][Settings.REVIEWS_COLLECTION]
userscore_collection = MongoClient(Settings.MONGO_CONNECTION_STRING)[Settings.TOPICS_DATABASE][Settings.USERSCORE_COLLECTION]
businessscore_collection = MongoClient(Settings.MONGO_CONNECTION_STRING)[Settings.TOPICS_DATABASE][Settings.BUSINESSSCORE_COLLECTION]


# Predict topic from review , use Topic model obtained by running LDA on all reviews
class Predict():
    def __init__(self):
        dictionary_path = "models/dictionary.dict"
        lda_model_path = "models/lda_model_50_topics.lda"
        self.dictionary = corpora.Dictionary.load(dictionary_path)
        self.lda = LdaModel.load(lda_model_path)

    def run(self, new_review):
        new_review_bow = self.dictionary.doc2bow(new_review)
        new_review_lda = self.lda[new_review_bow]
        return new_review_lda

# Worker Process to Run LDA on each User
def worker(identifier, start, end,itemList,itemType):
    #Intialize predict class
    predict=Predict()

    for i in range(start,end):
        itemId = itemList[i]
        itemScore=[]
        # create a userScore Array for 50 topics, Score has weightage for each topic
        # Intialize User score array
        for i in range(0,50):
            itemScore.append(0.0)

        reviewList=tags_collection.find({itemType:itemId})

        # Go throgh review of each user and run LDA on in it
        # Multiply topic score with Rating given to that review which infer whether User have
        # postive sentiment or negative sentiment about this topic aspect
        counter=0
        for review in reviewList:
            reviewLists = re.sub("[^\w]", " ",  review["text"]).split()
            ldaScore=predict.run(reviewLists)
            counter = counter + 1
            if counter % 1000 == 0:
                print 'Thread:'+str(identifier)+' '+str(counter) +' '+'record inserted for'+ str(itemType)+'Profile '

            reviewCount=0
            for topicScore in ldaScore:
                reviewCount = reviewCount + 1
                itemScore[topicScore[0]]= float(itemScore[topicScore[0]])+ float(topicScore[1])*float(review["rating"])

        # Normalize Userscore dividing by total Number of Reviews
        for i in range(0,50):
            itemScore[i]=float(itemScore[i])/float(reviewCount)
        # Insert the result in userscore collection
        if itemType == "userId":
            userscore_collection.insert({"userId":itemId , "userscore":itemScore})
        else:
            businessscore_collection.insert({"business":itemId , "businessscore":itemScore})

def main():
    #Set up log level as info
    logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

    # Cursor to extract userId and business from all reviews
    reviews_cursor = tags_collection.find()
    reviews_cursor.batch_size(1000)

    # Create userList and businessList to store unique User Id
    userList=[]
    businessList=[]

    # Go through all reviews in the Collection and Add userId and business to userList
    counter=0
    for review in reviews_cursor:
        userList.append(review["userId"])
        businessList.append(review["business"])
        counter = counter + 1
        if counter % 100000 == 0:
            print str(counter) + 'Record Read from reviews collection'

    # Remove duplicate business and userId from the List
    userList=Set(userList)
    userList=list(userList)
    businessList=Set(businessList)
    businessList=list(businessList)

    print 'Number of User in Dataset' + str(len(userList))
    print 'Number of Business in Dataset' + str(len(businessList))


    # Process User Review to create User Profile
    count = len(userList)
    workers = 6
    batch = int(count / workers)
    jobs = []
    for i in range(workers):
         p = multiprocessing.Process(target=worker, args=((i + 1), int(i * batch), int((i+1)*batch),userList,"userId"))
         jobs.append(p)
         p.start()
    for j in jobs:
         j.join()
         print '%s.exitcode = %s' % (j.name, j.exitcode)

    #Process Business Review to create Business Profile
    count = len(businessList)
    workers = 6
    batch = count / workers
    jobs = []
    for i in range(workers):
         p = multiprocessing.Process(target=worker, args=((i + 1), int(i * batch), int((i+1)*batch),businessList,"business"))
         jobs.append(p)
         p.start()
    for j in jobs:
         j.join()
         print '%s.exitcode = %s' % (j.name, j.exitcode)


if __name__ == '__main__':
    main()

