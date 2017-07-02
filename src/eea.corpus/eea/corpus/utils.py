#!/usr/bin/env python
# -*- coding: utf-8 -*-

# https://github.com/chartbeat-labs/textacy
# make sure you have downloaded the language model
# $ python -m spacy.en.download all

from __future__ import unicode_literals
import os


# from textacy.fileio import split_record_fields

CORPUS_STORAGE = "/corpus"


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
    print("Default column", column)
    return column
