import multiprocessing
import nltk
from pymongo import MongoClient
from settings import Settings


def load_stopwords():
    stopwords = {}
    with open('stopwords.txt', 'rU') as f:
        for line in f:
            stopwords[line.strip()] = 1

    return stopwords


def worker(identifier, skip, count):

    stopwords = load_stopwords()
    reviews_collection = MongoClient(Settings.MONGO_CONNECTION_STRING)[Settings.REVIEWS_DATABASE][
        Settings.REVIEWS_COLLECTION]
    tags_collection = MongoClient(Settings.MONGO_CONNECTION_STRING)[Settings.TAGS_DATABASE][
        Settings.REVIEWS_COLLECTION]

    reviews_cursor= reviews_collection.find()
    # Tags creation and storage is insert heavey operation , so Adding data in Bulk
    print tags_collection
    bulk = tags_collection.initialize_unordered_bulk_op();
    bulkCounter = 0
    counter = 0
    for review in reviews_cursor:
        words = []
        sentences = nltk.sent_tokenize(review["text"].lower())
        for sentence in sentences:
            tokens = nltk.word_tokenize(sentence)
            text = [word for word in tokens if word not in stopwords]
            tagged_text = nltk.pos_tag(text)
            for word, tag in tagged_text:
                words.append({"word": word, "pos": tag})
                bulk.insert({
                    "rating" : review ["rating"],
                    "userId" : review ["userId"],
                    "reviewId": review["reviewId"],
                    "business": review["business"],
                    "text": review["text"],
                    "words": words
                    })
                bulkCounter=bulkCounter + 1
                counter = counter + 1
                if bulkCounter % 10000 == 0 :
                    bulk.execute()
                    print str(counter) + 'Entries are inserted in Thread' + str(multiprocessing.current_process())
                    bulkCounter=0
                    bulk= tags_collection.initialize_unordered_bulk_op()
    if bulkCounter > 0 :
        bulk.execute()

def main():
    reviews_collection = MongoClient(Settings.MONGO_CONNECTION_STRING)[Settings.REVIEWS_DATABASE][
        Settings.REVIEWS_COLLECTION]
    reviews_cursor = reviews_collection.find()
    count = reviews_cursor.count()
    workers = 6
    batch = count / workers

    jobs = []
    for i in range(workers):
        p = multiprocessing.Process(target=worker, args=((i + 1), i * batch, count / workers))
        jobs.append(p)
        p.start()

    for j in jobs:
        j.join()
        print '%s.exitcode = %s' % (j.name, j.exitcode)


if __name__ == '__main__':
    main()
