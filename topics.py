"""
Each topic is a list of tuples. Each tuple is the weight of the term in the
topic distribution, followed by the term id, as a string. In order to convert to
the term, the id must be converted to an int and looked up in the dictionary.
"""

import csv
from gensim import corpora, models


model = ...

def get_top_n_topics(model, dict, n, topn=20):
    topics = []
    for topic_num in range(n):
        topic = [(dict[int(term_id)], float(weight))
                 for weight, term_id in model.show_topic(topic_num, topn)]
        topics.append(topic)
    return topics

def write_topics(topics, dirname):
    try:
        os.mkdir(dirname)
    except OSError:
        pass

    curdir = os.getcwd()
    os.chdir(dirname)
    for topic_num, topic in enumerate(topics):
        with open('%s.csv' % topic_num, 'w') as f:
            for row in topic:
                f.write((u'%s,%f\n' % row).encode('utf-8'))
    os.chdir(curdir)
