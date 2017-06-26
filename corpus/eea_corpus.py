#!/usr/bin/env python
# -*- coding: utf-8 -*-

# https://github.com/chartbeat-labs/textacy
# make sure you have downloaded the language model
# $ python -m spacy.en.download all

from __future__ import unicode_literals
from bs4 import BeautifulSoup
from textacy.fileio import split_record_fields
import os
import phrasemachine
import textacy

CORPUS_NAME = 'eeacorpus'


class EEACorpus(object):
    """
    Stream EEA Corpus.
    """

    def __repr__(self):
        s = 'EEA Corpus'
        return s

    def create_corpus(self, fpath, text_column='text', normalize=False,
                      optimize_phrases=False):
        """
        Load csv file from fpath. Each row is one document.
        It expects first column to be the Text / Body we want to analyse with
        textacy. The rest of the columns are stored as metadata associated
        to each document.

        If normalize is set to True many aspects of the text will be
        normalized like bad unicode, currency symbols, phone numbers, urls,
        emails, punctuations, accents etc.
        see textacy.preprocess.preprocess_text for details.

        Returns a textacy.Corpus.
        """

        # read all eea documents from csv file
        eeadocs = textacy.fileio.read.read_csv(fpath)

        # use first column from the csv file as the text to analyse.
        # the rest is metadata
        content_stream, metadata_stream = split_record_fields(eeadocs,
                                                              text_column)

        if normalize:
            content_stream = self._normalize_content_stream(content_stream,
                                                            optimize_phrases)

        # create textacy english Corpus
        corpus = textacy.Corpus('en', texts=content_stream,
                                metadatas=metadata_stream)

        return corpus

    def load_or_create_corpus(self, fpath, text_column='text', normalize=False,
                              optimize_phrases=False):
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
        corpus = self.create_corpus(fpath, normalize, optimize_phrases)
        corpus.save(cpath, name=CORPUS_NAME)

        return corpus

    def _tokenize_phrases(self, content):
        """
        Find phrases in content via phrasemachine and return content where
        phrases are tokenized with '_' like 'air_pollution' so that they can
        be treated as new words.
        """
        # get phrases
        phrases = phrasemachine.get_phrases(content)
        tokens = [p.replace(' ', '_') for p in phrases['counts']]
        return tokens

    def _normalize_content_stream(self, content_stream,
                                  optimize_phrases=False):
        """
        Iterate over the content, yielding one normalized content.

        Many aspects of the text will be normalized like bad unicode, currency
        symbols, phone numbers, urls, emails, punctuations, accents etc.

        Yields:
            str: normalized plain text for the next document.
        """
        i = 0
        for content in content_stream:
            i += 1
            if i % 100 == 0:
                print(str(i))   # show progress in terminal

            # first let us clean any html code
            soup = BeautifulSoup(content, 'html.parser')
            content = soup.get_text()

            # then pre-proccess via textacy
            content = textacy.preprocess.preprocess_text(
                content,
                fix_unicode=True, lowercase=False, transliterate=True,
                no_urls=True, no_emails=True, no_phone_numbers=True,
                no_numbers=True, no_currency_symbols=True, no_punct=False,
                no_contractions=True, no_accents=True
            )

            # we are only interested in high level concepts
            if optimize_phrases:
                content = ' '.join(self._tokenize_phrases(content))

            yield content
