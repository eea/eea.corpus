#!/usr/bin/env python
# -*- coding: utf-8 -*-

# https://github.com/chartbeat-labs/textacy
# make sure you have downloaded the language model
# $ python -m spacy.en.download all

from __future__ import print_function
from pyLDAvis import show
import click
import eea_corpus
import textacy
import vis

# These are the metadata columns
# [(0, u'label'),
#  (1, u'expires'),
#  (2, u'description'),
#  (3, u'issued'),
#  (4, u'modified'),
#  (5, u'Regions/Places/Cities/Seas...'),
#  (6, u'Countries'),
#  (7, u'WorkflowState'),
#  (8, u'topics'),
#  (9, u'url'),
#  (10, u'Content types'),
#  (11, u'Time coverage'),
#  (12, u'format'),
#  (13, u'organisation'),
#  (14, u'language')]


def published_match_func(doc):
    return doc.metadata[7] == 'published'


def url_match_func(url):
    return lambda doc: doc.metadata[9] == url


def extras(corpus):
    print('Corpus: ', corpus)

    # find published docs
    for doc in corpus.get(published_match_func, limit=3):
        triples = textacy.extract.subject_verb_object_triples(doc)
        print('Published doc: ', doc, list(triples))

    # find doc with specific url
    url = 'http://www.eea.europa.eu/publications/C23I92-826-5409-5'
    for doc in corpus.get(url_match_func(url), limit=3):
        print('specific url:', doc)

    # get terms list
    for doc in corpus.get(url_match_func(url), limit=3):
        tlist = doc.to_terms_list(
            ngrams=1, named_entities=True, as_strings=True
        )
        terms = list(tlist)
        print(terms)


@click.command()
@click.option(
    '--normalize', is_flag=True, default=False,
    help="Normalize text. Warning, heaving processing."
)
@click.option(
    '--data', default='data.csv', help="Path to CSV file to process")
def main(normalize, data):
    print("Processing file {} with normalize {}".format(data, normalize))
    ec = eea_corpus.EEACorpus()
    corpus = ec.load_or_create_corpus(fpath=data, normalize=normalize)

    extras(corpus)

    # Represent corpus as a document-term matrix, with flexible weighting and
    # filtering:
    docs = (
        doc.to_terms_list(ngrams=1, named_entities=True, as_strings=True)
        for doc in corpus
    )
    doc_term_matrix, id2term = textacy.vsm.doc_term_matrix(
        docs, weighting='tfidf', normalize=True, smooth_idf=True, min_df=2,
        max_df=0.95, max_n_terms=100000)

    print('DTM: ', repr(doc_term_matrix))

    # Train and interpret a topic model:
    model = textacy.tm.TopicModel('lda', n_topics=20)
    model.fit(doc_term_matrix)

    # Transform the corpus and interpret our model:
    doc_topic_matrix = model.transform(doc_term_matrix)

    print('DocTopicMatrix shape', doc_topic_matrix.shape)

    print('Discovered topics:')
    for topic_idx, top_terms in model.top_topic_terms(id2term, top_n=10):
        print('topic', topic_idx, ':', '   '.join(top_terms))

    # Show top 2 doc within first 2 topics
    top_topic_docs = model.top_topic_docs(
        doc_topic_matrix,
        topics=[0, 1],
        top_n=2
    )
    for (topic_idx, top_docs) in top_topic_docs:
        print(topic_idx)
        for j in top_docs:
            print(corpus[j].metadata[8])
            print(corpus[j])

    prep_data = vis.prepare(model.model, doc_term_matrix, id2term)
    show(prep_data, ip="0.0.0.0", port=8888)

    # model.save('model.saved')
    # model.termite_plot(doc_term_matrix, id2term, topics=-1, n_terms=2,
    #                    sort_terms_by="seriation")


if __name__ == "__main__":
    main()
