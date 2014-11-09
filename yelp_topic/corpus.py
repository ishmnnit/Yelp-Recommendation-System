from pymongo import MongoClient
from nltk.stem.wordnet import WordNetLemmatizer
from settings import Settings

tags_collection = MongoClient(Settings.MONGO_CONNECTION_STRING)[Settings.TAGS_DATABASE][Settings.REVIEWS_COLLECTION]
corpus_collection = MongoClient(Settings.MONGO_CONNECTION_STRING)[Settings.TAGS_DATABASE][Settings.CORPUS_COLLECTION]

reviews_cursor = tags_collection.find()
reviewsCount = reviews_cursor.count()
reviews_cursor.batch_size(5000)

lem = WordNetLemmatizer()

## Do Bulk insertion
bulk = corpus_collection.initialize_unordered_bulk_op()
bulkCounter=0
counter=0

for review in reviews_cursor:
    nouns = []
    words = [word for word in review["words"] if word["pos"] in ["NN", "NNS"]]

    for word in words:
        nouns.append(lem.lemmatize(word["word"]))

    bulk.insert({
        "userId" : review["userId"],
        "rating" : review["rating"],
        "reviewId": review["reviewId"],
        "business": review["business"],
        "text": review["text"],
        "words": nouns
    })
    bulkCounter = bulkCounter + 1
    if bulkCounter % 100000 == 0 :
        bulk.execute()
        print  str(counter)+ "Entries are inserted"
        bulkCounter=0
        counter=counter+1
        bulk = corpus_collection.initialize_unordered_bulk_op()

if bulkCounter > 0:
    bulk.execute()

