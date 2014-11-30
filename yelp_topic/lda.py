from __future__ import division
import graphlab as gl
import os
from pymongo import MongoClient
from settings import Settings
from sets import Set


# Intialize all collection
tags_collection = MongoClient(Settings.MONGO_CONNECTION_STRING)[Settings.TAGS_DATABASE][Settings.REVIEWS_COLLECTION]
userscore_collection = MongoClient(Settings.MONGO_CONNECTION_STRING)[Settings.GRAPHLAB_DATABASE][Settings.USERSCORE_COLLECTION]
businessscore_collection = MongoClient(Settings.MONGO_CONNECTION_STRING)[Settings.GRAPHLAB_DATABASE][Settings.BUSINESSSCORE_COLLECTION]


def create():

    docs = gl.SFrame.read_csv('/home/ish/DataScience/yelp_topic/dataset/yelp_dataset_challenge_academic_dataset',header=False, delimiter='\n', column_type_hints=dict)
    # Convert JSON File into CSV ,on the basis of the  columns
    docs1 = docs.unpack('X1', column_name_prefix='', limit=['user_id', 'business_id', 'text'])
    #  Count Number of word , take input in SArray format
    docs1 = gl.text_analytics.count_words(docs1['text'])
    # Remove Stopword
    docs1 = docs1.dict_trim_by_keys(gl.text_analytics.stopwords(), exclude=True)
    # Learn topic model
    m = gl.topic_model.create(docs1)
    m.save('ldamodel')
    return m

# Predict topic from review , use Topic model obtained by running LDA on all reviews

def run(review, lda):
    #Convert List into SArray

    lst=[]
    lst.append(review)
    new_review_array = gl.SArray(lst)

    new_review_bow = gl.text_analytics.count_words(new_review_array, to_lower=True)
    new_review_bow = new_review_bow.dict_trim_by_keys(gl.text_analytics.stopwords(), exclude=True)

    # Get the Topic probability
    new_review_lda = lda.predict(new_review_bow, output_type='probability')
    return new_review_lda

# Worker Process to Run LDA on each User
def worker(identifier, start, end,itemList,itemType, lda):
    #Intialize predict class
    if itemType == "userId":
        bulk = userscore_collection.initialize_unordered_bulk_op()
    else:
        bulk = businessscore_collection.initialize_unordered_bulk_op()

    bulkCounter=0
    for i in range(start,end):
        itemId = itemList[i]
        itemScore=[]
        # create a userScore Array for 50 topics, Score has weightage for each topic
        # Intialize User score array
        for i in range(0,10):
            itemScore.append(0.0)

        reviewList=tags_collection.find({itemType:itemId})

        # Go throgh review of each user and run LDA on in it
        # Multiply topic score with Rating given to that review which infer whether User have
        # postive sentiment or negative sentiment about this topic aspect
        counter=0
        reviewCount=0
        for review in reviewList:
            ldaScore=run(review['text'], lda)
            counter = counter + 1
            if counter % 1000 == 0:
                print 'Thread:'+str(identifier)+' '+str(counter) +' '+'record inserted for'+ str(itemType)+'Profile '

            for topicScore in ldaScore:
                i=0
                for t in topicScore:
                    itemScore[i]= float(itemScore[i])+ float(t)*float(review["rating"])
                    i=i+1
            reviewCount = reviewCount + 1

        # Normalize Userscore dividing by total Number of Reviews
        for i in range(0,10):
            itemScore[i]=float(itemScore[i])/float(reviewCount)
        # Insert the result in userscore collection
        if itemType == "userId":
            bulk.insert({"userId":itemId , "userscore":itemScore})
        else:
            bulk.insert({"business":itemId , "businessscore":itemScore})

        bulkCounter = bulkCounter + 1
        if bulkCounter > 0 :
            bulk.execute()
            bulkCounter=0
            if itemType == "userId":
                bulk = userscore_collection.initialize_unordered_bulk_op()
            else:
                bulk = businessscore_collection.initialize_unordered_bulk_op()



    if bulkCounter > 0 :
        bulk.execute()


def main():

    # Cursor to extract userId and business from all reviews
    reviews_cursor = tags_collection.find()
    reviews_cursor.batch_size(1000)

    # Create userList and businessList to store unique User Id
    userList=[]
    businessList=[]

    #Create an LDA model from graphlab
    if os.path.exists("ldamodel"):
        print "reading from the file"
        lda=gl.load_model("ldamodel")
    else:
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
    worker(1, 0, count,userList,"userId", lda)


    #Process Business Review to create Business Profile
    count = len(businessList)
    worker(1, 0, count,businessList,"business", lda)


if __name__ == '__main__':
    main()
