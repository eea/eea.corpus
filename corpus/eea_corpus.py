#!/usr/bin/env python
# -*- coding: utf-8 -*-

# https://github.com/chartbeat-labs/textacy
# make sure you have downloaded the language model
# $ python -m spacy.en.download all

from __future__ import unicode_literals
import os
import textacy

CORPUS_NAME = 'eeacorpus'


class EEACorpus(object):
    """
    Stream EEA Corpus.
    """

    def __repr__(self):
        s = 'EEA Corpus'
        return s

    def create_corpus(self, fpath):
        """
        Load csv file from fpath. Each row is one document.
        It expects first column to be the Text / Body we want to analyse with
        textacy. The rest of the columns are stored as metadata associated
        to each document.

        Returns a textacy.Corpus.
        """

        # read all eea documents from csv file
        eeadocs = textacy.fileio.read.read_csv(fpath)

        # use first column from the csv file as the text to analyse.
        # the rest is metadata
        content_stream, metadata_stream = textacy.fileio.split_record_fields(
            eeadocs, 0)

        # create textacy english Corpus
        corpus = textacy.Corpus('en', texts=content_stream,
                                metadatas=metadata_stream)

        return corpus

    def load_or_create_corpus(self, fpath):
        base, fname = os.path.split(fpath)
        varpath = os.path.join(base, 'var')
        cpath = os.path.join(varpath, fname)

        if not os.path.exists(cpath):
            os.makedirs(cpath)

        if os.listdir(cpath):
            # if there are any files, assume the corpus is created
            print("Saved corpus exists, loading", cpath)
            return textacy.Corpus.load(cpath, name=CORPUS_NAME)

        print('Creating corpus', fpath, cpath)
        corpus = self.create_corpus(fpath)
        # corpus.save(cpath, name=CORPUS_NAME)

        return corpus
