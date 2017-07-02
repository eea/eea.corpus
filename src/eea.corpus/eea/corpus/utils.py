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
CORPUS_STORAGE = "/corpus"


def build_corpus(file_name, text_column='text', normalize=False,
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

    document_path = upload_location(file_name)
    df = pd.read_csv(document_path)
    content_stream = df[text_column].__iter__()

    if normalize:
        content_stream = _normalize_content_stream(content_stream,
                                                   optimize_phrases)

    corpus = textacy.Corpus('en', texts=content_stream)

    return corpus


def corpus_base_path(file_name):
    """ Returns the /corpus/var/<filename> folder for an uploaded file
    """
    varpath = os.path.join(CORPUS_STORAGE, 'var')
    base = os.path.join(varpath, file_name)
    return base


def corpus_path(file_name, text_column):
    """ Returns the directory for a corpus based on file name and column
    """

    base = corpus_base_path(file_name)
    cpath = os.path.join(base, text_column)

    if not os.path.exists(cpath):
        os.makedirs(cpath)

    return cpath


def upload_location(file_name):
    assert not file_name.startswith('.')
    return os.path.join(CORPUS_STORAGE, file_name)


def is_valid_document(file_name):
    return file_name in os.listdir(CORPUS_STORAGE)


def available_columns(file_name):
    """ Returns available, already-created, corpuses for a filename

    The corpuses corespond to a column in the file.
    """
    base = corpus_base_path(file_name)
    return os.path.exists(base) and os.listdir(base)


def available_documents():
    """ Returns a list of available files in the big corpus storage
    """
    existing = [f for f in os.listdir(CORPUS_STORAGE) if f.endswith('.csv')]
    return existing


def load_or_create_corpus(file_name, text_column='text', normalize=False,
                          optimize_phrases=False):

    cpath = corpus_path(file_name, text_column)
    if os.listdir(cpath):
        # if there are any files, assume the corpus is created
        print("Saved corpus exists, loading", cpath)
        return textacy.Corpus.load(cpath, name=CORPUS_NAME)

    print('Creating corpus', file_name, cpath)
    corpus = build_corpus(file_name, text_column=text_column,
                          normalize=normalize,
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
    phrases = phrasemachine.get_phrases(content)        # , tagger='spacy'
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

        if not isinstance(content, str):
            continue

        # first let us clean any html code
        # print("Cleaning up", content[:40])
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


def document_name(request):
    """ Extract document name (aka file_name) from request
    """

    md = request.matchdict or {}
    fname = md.get('name')
    return is_valid_document(fname) and fname


def default_column(file_name, request):
    """ Identify the "default" column.

    * If a given column name is given in request, use that.
    * if not, identify it the corpus folder has any folders for columns.
        Use the first available such column
    """
    column = request.params.get('column') or ''
    cache = request.corpus_cache

    # if there's no column, try to identify a column from the cache
    if not column:
        columns = list(cache.get(file_name, {}))
        if columns:
            column = columns[0]     # grab the first cached

    # if there's no column, try to identify a column from the var dir
    columns = available_columns(file_name)
    column = columns and columns[0] or 'text'
    return column
