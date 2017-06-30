#!/usr/bin/env python
# -*- coding: utf-8 -*-

# https://github.com/chartbeat-labs/textacy
# make sure you have downloaded the language model
# $ python -m spacy.en.download all

from __future__ import print_function
from io import StringIO
from pyLDAvis import save_html
from pyLDAvis.sklearn import prepare
import textacy
# from eea.corpus import vis


class Vectorizer(textacy.vsm.Vectorizer):
    def get_feature_names(self):
        return self.feature_names


def build_model(corpus, topics, num_docs=None, min_df=0.1, max_df=0.7):
    docs = (
        doc.to_terms_list(ngrams=1, named_entities=False, as_strings=True)
        for doc in corpus[:num_docs]
    )

    vectorizer = Vectorizer(
        weighting='tfidf',
        normalize=False, smooth_idf=False, min_df=min_df, max_df=max_df,
        max_n_terms=100000
    )
    doc_term_matrix = vectorizer.fit_transform(docs)

    print('DTM: ', repr(doc_term_matrix))

    # Train and interpret a topic model:
    model = textacy.tm.TopicModel('lda', n_topics=topics)
    model.fit(doc_term_matrix)

    # Transform the corpus and interpret our model:
    doc_topic_matrix = model.transform(doc_term_matrix)

    print('DocTopicMatrix shape', doc_topic_matrix.shape)

    # print('Discovered topics:')
    # for topic_idx, top_terms in model.top_topic_terms(id2term, top_n=10):
    #     print('topic', topic_idx, ':', '   '.join(top_terms))

    # Show top 2 doc within first 2 topics
    # top_topic_docs = model.top_topic_docs(
    #     doc_topic_matrix,
    #     topics=[0, 1],
    #     top_n=2
    # )
    # for (topic_idx, top_docs) in top_topic_docs:
    #     print(topic_idx)
    #     for j in top_docs:
    #         print(corpus[j].metadata[8])
    #         print(corpus[j])

    return model, doc_term_matrix, vectorizer


def pyldavis_visualization(corpus, topics, num_docs=None, min_df=0.1,
                           max_df=0.7, mds='pcoa'):
    model, doc_term_matrix, vectorizer = build_model(corpus, topics, num_docs,
                                                     min_df, max_df)
    prep_data = prepare(model.model, doc_term_matrix, vectorizer, mds=mds)
    out = StringIO()
    save_html(prep_data, out)
    out.seek(0)
    return out.read()


def termite_visualization(corpus, topics, num_docs=None, min_df=0.1,
                          max_df=0.7, *args, **kwargs):
    model, doc_term_matrix, vectorizer = build_model(corpus, topics, num_docs,
                                                     min_df, max_df)
    out = StringIO()
    id2term = dict(
        zip(vectorizer.vocabulary.values(), vectorizer.vocabulary.keys())
    )
    model.termite_plot(doc_term_matrix, id2term, save=out)
    out.seek(0)
    bs = out.read()
    return bs
