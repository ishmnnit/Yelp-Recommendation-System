import logging
from gensim.models import LdaModel
from gensim import corpora
import nltk
from nltk.stem.wordnet import WordNetLemmatizer

btopics_collection= MongoClient(Settings.MONGO_CONNECTION_STRING)
[Settings.TOPICS_DATABASE][Settings.btopics_COLLECTION]

businesstopics_collection= MongoClient(Settings.MONGO_CONNECTION_STRING)
[Settings.TOPICS_DATABASE][Settings.businesstopics_COLLECTION]

tags_collection = MongoClient(Settings.MONGO_CONNECTION_STRING)[Settings.TAGS_DATABASE][
            Settings.tags_collection]

reviews_cursor = tags_collection.find()
reviews_cursor.batch_size(1000)

class Predict():
    def __init__(self):
        dictionary_path = "models/dictionary.dict"
        lda_model_path = "models/lda_model_50_topics.lda"
        self.dictionary = corpora.Dictionary.load(dictionary_path)
        self.lda = LdaModel.load(lda_model_path)

    def run(self, new_review):
        new_review_bow = self.dictionary.doc2bow(nouns)
        new_review_lda = self.lda[new_review_bow]
        return new_review_lda


def main():
    logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

    # Append Review of every user , to extract topics from user reviews
    for review in reviews_cursor:
        btopic_cursor=btopics_collection.find({"business":review["business"]})
        if btopic_cursor.count() == 0 :
            btopics.collection.insert({"business":review["business"],"text":review["text"]})
        else:
            userReview= review["text"]
            for breview in btopic_cursor:
                userReview += breview
            btopics.collection.update({"business":review["business"],"text":review["text"]})

    # Extract topics from User review
    businesstopics_cursor=btopics_collection.find()

    for breview in businesstopics_cursor:
        predict = Predict()
        result = predict.run(breview["text"])
        businesstopics.collection.insert({"business":breview["business"],"topics":result})


if __name__ == '__main__':
    main()


