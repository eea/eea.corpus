#!/usr/bin/env python
# -*- coding: utf-8 -*-

# https://github.com/chartbeat-labs/textacy
# make sure you have downloaded the language model
# $ python -m spacy.en.download all

from __future__ import unicode_literals
import os
import textacy
from bs4 import BeautifulSoup
import phrasemachine

CORPUS_NAME = 'eeacorpus'


class EEACorpus(object):
    """
    Stream EEA Corpus.
    """

    def __repr__(self):
        s = 'EEA Corpus'
        return s

    def create_corpus(self, fpath, normalise=False):
        """
        Load csv file from fpath. Each row is one document.
        It expects first column to be the Text / Body we want to analyse with
        textacy. The rest of the columns are stored as metadata associated
        to each document.
        
        If normalise is set to True many aspects of the text will be 
        normalised like bad unicode, currency symbols, phone numbers, urls, 
        emails, punctuations, accents etc. 
        see textacy.preprocess.preprocess_text for details.
        
        Returns a textacy.Corpus.
        """

        # read all eea documents from csv file
        eeadocs = textacy.fileio.read.read_csv(fpath)

        # use first column from the csv file as the text to analyse.
        # the rest is metadata
        content_stream, metadata_stream = textacy.fileio.split_record_fields(
            eeadocs, 0)
            
        if normalise:
            content_stream = self._normalise_content_stream(content_stream)

        # create textacy english Corpus
        corpus = textacy.Corpus('en', texts=content_stream,
                                metadatas=metadata_stream)

        return corpus

    def load_or_create_corpus(self, fpath, normalise=False):
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
        corpus = self.create_corpus(fpath, normalise)
        # corpus.save(cpath, name=CORPUS_NAME)

        return corpus

    def _tokenize_phrases(self, content):
        """
        Find phrases in content via phrasemachine and return content where
        phrases are tokenized with '_' like 'air_pollution' so that they can
        be treated as new words. 
        """
        # TODO: split content into sentences list
        # for each sentence get phrases
        # include only phrase that have max 3 words (customisable) 
        # ignoring stopwords
        # optionally, if vocabulary provided (GEMET, EEA tags/glossary), ignore
        # phrases that do not contain any vocabulary term of from it.
        # get phrases
        phrases = phrasemachine.get_phrases(content)
        tokens = [p.replace(' ','_') for p in phrases['counts']]
        return tokens

    def _normalise_content_stream(self, content_stream):
        """
        Iterate over the content, yielding one normalised content.
        
        Many aspects of the text will be normalised like bad unicode, 
        currency symbols, phone numbers, urls, emails, punctuations, accents etc. 
            
        Yields:
            str: normalised plain text for the next document. 
        """
        i = 0
        for content in content_stream:
            i +=1
            if i % 100 == 0:
                print(str(i)) # show progress in terminal
                
            # first let us clean any html code
            soup = BeautifulSoup(content, 'html.parser')
            content = soup.get_text()
            
            # then pre-proccess via textacy
            content = textacy.preprocess.preprocess_text(content, 
                    fix_unicode=True, lowercase=False, transliterate=True, 
                    no_urls=True, no_emails=True, no_phone_numbers=True, 
                    no_numbers=True, no_currency_symbols=True, no_punct=False,
                    no_contractions=True, no_accents=True)
            
            # we are only interested in high level concepts
            content = ' '.join(self._tokenize_phrases(content))
            
            yield content