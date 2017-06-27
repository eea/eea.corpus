#!/usr/bin/env python
# -*- coding: utf-8 -*-

# https://github.com/chartbeat-labs/textacy
# make sure you have downloaded the language model
# $ python -m spacy.en.download all

from __future__ import unicode_literals
from bs4 import BeautifulSoup
import os
import pandas as pd
import phrasemachine
import textacy
import logging

logger = logging.getLogger('eea.corpus')


# from textacy.fileio import split_record_fields

CORPUS_NAME = 'eeacorpus'


def create_corpus(fpath, text_column='text', normalize=False,
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

    # # read all eea documents from csv file
    # eeadocs = textacy.fileio.read.read_csv(fpath)
    #
    # # use first column from the csv file as the text to analyse.
    # # the rest is metadata
    # content_stream, metadata_stream = split_record_fields(eeadocs,
    #                                                       text_column)

    df = pd.read_csv(fpath)
    content_stream = df[text_column].__iter__()

    if normalize:
        content_stream = _normalize_content_stream(content_stream,
                                                   optimize_phrases)

    corpus = textacy.Corpus('en', texts=content_stream)

    return corpus


def load_or_create_corpus(fpath, text_column='text', normalize=False,
                          optimize_phrases=False):
    base, fname = os.path.split(fpath)
    # fname = fname + '-' + text_column
    # fpath = os.path.join(base, fname)

    varpath = os.path.join(base, 'var')
    cpath = os.path.join(varpath, fname, text_column)

    if not os.path.exists(cpath):
        os.makedirs(cpath)

    if os.listdir(cpath):
        # if there are any files, assume the corpus is created
        print("Saved corpus exists, loading", cpath)
        return textacy.Corpus.load(cpath, name=CORPUS_NAME)

    print('Creating corpus', fpath, cpath)
    corpus = create_corpus(fpath, text_column=text_column, normalize=normalize,
                           optimize_phrases=optimize_phrases)
    corpus.save(cpath, name=CORPUS_NAME)

    return corpus


def _tokenize_phrases(content):
    """
    Find phrases in content via phrasemachine and return content where
    phrases are tokenized with '_' like 'air_pollution' so that they can
    be treated as new words.
    """
    # get phrases
    phrases = phrasemachine.get_phrases(content)
    tokens = [p.replace(' ', '_') for p in phrases['counts']]
    return tokens


def _normalize_content_stream(content_stream, optimize_phrases=False):
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

        if i > 100:
            break

        if not isinstance(content, str):
            continue

        # first let us clean any html code
        try:
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
        except Exception:
            logger.warning("Got an error in extracting content: %r", content)
            import pdb; pdb.set_trace()
            continue

        # we are only interested in high level concepts
        if optimize_phrases:
            content = ' '.join(_tokenize_phrases(content))

        yield content
