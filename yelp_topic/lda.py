from __future__ import division
import pdb
import graphlab as gl
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
    

def create():

    docs = gl.SFrame.read_csv('yelp_academic_dataset_review.json',
                header=False, delimiter='\n', column_type_hints=dict)
#print docs
    docs1 = docs.unpack('X1', column_name_prefix='', limit=['user_id', 'business_id', 'text'])
#print docs1

# Remove stopwords and convert to bag of words
    docs1 = gl.text_analytics.count_words(docs1['text'])
    docs1 = docs1.dict_trim_by_keys(gl.text_analytics.stopwords(), exclude=True)

# Learn topic model
    m = gl.topic_model.create(docs1)
    return m

# Predict topic from review , use Topic model obtained by running LDA on all reviews
class Predict():

    def run(self, reviewList, lda):
	#print new_review['text'] 
        #pdb.set_trace()	
	new_review_array = gl.SArray(reviewList)
	#print new_review_array
        new_review_bow = gl.text_analytics.count_words(new_review_array, to_lower=True)
        new_review_bow = new_review_bow.dict_trim_by_keys(gl.text_analytics.stopwords(), exclude=True)
        new_review_lda = lda.predict(new_review_bow, output_type='probability')
	print new_review_lda
        return new_review_lda

# Worker Process to Run LDA on each User
def worker(identifier, start, end,itemList,itemType, lda):
    #Intialize predict class
    predict=Predict()

    for i in range(start,end):
        itemId = itemList[i]
        itemScore=[]
        # create a userScore Array for 50 topics, Score has weightage for each topic
        # Intialize User score array
        for i in range(0,50):
            itemScore.append(0.0)

        reviewList=tags_collection.find({itemType:itemId}).limit(100)

        # Go throgh review of each user and run LDA on in it
        # Multiply topic score with Rating given to that review which infer whether User have
        # postive sentiment or negative sentiment about this topic aspect
        counter=0
        for review in reviewList:
            #reviewLists = re.sub("[^\w]", " ",  review["text"]).split()
            ldaScore=predict.run(review['text'], lda)
	    print "predict done"
            counter = counter + 1
            if counter % 1000 == 0:
                print 'Thread:'+str(identifier)+' '+str(counter) +' '+'record inserted for'+ str(itemType)+'Profile '

            reviewCount=0
            for topicScore in ldaScore:
                reviewCount = reviewCount + 1
                itemScore[topicScore[0]]= float(itemScore[topicScore[0]])+ float(topicScore[1])*float(review["rating"])
	print "Score calc done"
        # Normalize Userscore dividing by total Number of Reviews
        for i in range(0,50):
            itemScore[i]=float(itemScore[i])/float(reviewCount)
        # Insert the result in userscore collection
        if itemType == "userId":
            userscore_collection.insert({"userId":itemId , "userscore":itemScore})
        else:
            businessscore_collection.insert({"business":itemId , "businessscore":itemScore})
	print "insertion done"

def main():
    #Set up log level as info
    logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

    # Cursor to extract userId and business from all reviews
    reviews_cursor = tags_collection.find()
    reviews_cursor.batch_size(1000)

    # Create userList and businessList to store unique User Id
    userList=[]
    businessList=[]
    #Create an LDA model from graphlab
    lda = create()
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
         p = multiprocessing.Process(target=worker, args=((i + 1), int(i * batch), int((i+1)*batch),userList,"userId", lda))
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
         p = multiprocessing.Process(target=worker, args=((i + 1), int(i * batch), int((i+1)*batch),businessList,"business", lda))
         jobs.append(p)
         p.start()
    for j in jobs:
         j.join()
         print '%s.exitcode = %s' % (j.name, j.exitcode)


if __name__ == '__main__':
    main()
