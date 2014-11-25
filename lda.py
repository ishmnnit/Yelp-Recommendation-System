import graphlab as gl


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
print m.get_topics()
