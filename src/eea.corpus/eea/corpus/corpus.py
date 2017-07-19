from eea.corpus.async import queue
from eea.corpus.processing import build_pipeline
from eea.corpus.utils import corpus_base_path
from eea.corpus.utils import metadata
from rq.decorators import job
import json
import logging
import os.path
import textacy


logger = logging.getLogger('eea.corpus')


def save_corpus_metadata(corpus, file_name, corpus_id, text_column, **kw):
    cpath = corpus_base_path(file_name)      # corpus_id
    meta_name = "{0}_eea.json".format(corpus_id)
    meta_path = os.path.join(cpath, meta_name)

    title = kw.pop('title')
    description = kw.pop('description', '')

    info = {
        'title': title,
        'description': description,
        'metadata': metadata(corpus),
        'text_column': text_column,
        'kw': kw,
    }
    with open(meta_path, 'w') as f:
        json.dump(info, f)


@job(queue=queue)
def build_corpus(pipeline, corpus_id, file_name, text_column, **kw):
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

    cpath = corpus_base_path(file_name)      # corpus_id
    logger.info('Creating corpus for %s at %s', file_name, cpath)

    content_stream = build_pipeline(file_name, text_column, pipeline)

    corpus = textacy.Corpus('en', texts=content_stream)
    corpus.save(cpath, name=corpus_id)
    save_corpus_metadata(corpus, file_name, corpus_id, text_column, **kw)


# import phrasemachine
# def _tokenize_phrases(content):
#     """
#     Find phrases in content via phrasemachine and return content where
#     phrases are tokenized with '_' like 'air_pollution' so that they can
#     be treated as new words.
#     """
#     phrases = phrasemachine.get_phrases(content)        # , tagger='spacy'
#     tokens = [p.replace(' ', '_') for p in phrases['counts']]
#     return tokens
#
# from bs4 import BeautifulSoup
# from eea.corpus.utils import upload_location
# from textacy.preprocess import preprocess_text
# import inspect
# import sys
# @job(queue=queue)
# def build_corpus(corpus_id, file_name, text_column, **kw):
#     """
#     Load csv file from fpath. Each row is one document.
#     It expects first column to be the Text / Body we want to analyse with
#     textacy. The rest of the columns are stored as metadata associated
#     to each document.
#
#     If normalize is set to True many aspects of the text will be
#     normalized like bad unicode, currency symbols, phone numbers, urls,
#     emails, punctuations, accents etc.
#     see textacy.preprocess.preprocess_text for details.
#
#     Returns a textacy.Corpus.
#     """
#
#     cpath = corpus_base_path(file_name)      # corpus_id
#     logger.info('Creating corpus for %s at %s', file_name, cpath)
#
#     # # read all eea documents from csv file
#     # eeadocs = textacy.fileio.read.read_csv(fpath)
#     #
#     # # use first column from the csv file as the text to analyse.
#     # # the rest is metadata
#     # content_stream, metadata_stream = split_record_fields(eeadocs,
#     #                                                       text_column)
#
#     document_path = upload_location(file_name)
#     df = pd.read_csv(document_path)
#     content_stream = df[text_column].__iter__()
#
#     normalize = kw.pop('normalize', False)
#     if normalize:
#         content_stream = _normalize_content_stream(content_stream, **kw)
#
#     corpus = textacy.Corpus('en', texts=content_stream)
#     corpus.save(cpath, name=corpus_id)
#     save_corpus_metadata(corpus, file_name, corpus_id, text_column, **kw)
#
# # TODO: cleanup this code, it's no longer used
# def _normalize_content_stream(content_stream, **kw):
#     """
#     Iterate over the content, yielding one normalized content.
#
#     Many aspects of the text will be normalized like bad unicode, currency
#     symbols, phone numbers, urls, emails, punctuations, accents etc.
#
#     Yields:
#         str: normalized plain text for the next document.
#     """
#
#     textacy_preprocess_args = inspect.getargspec(preprocess_text).args
#     t_args = {}
#     for k, v in kw.get('textacy_pipeline', {}).items():
#         if k in textacy_preprocess_args:
#             t_args[k] = v
#
#     i = 0
#     for content in content_stream:
#         if (i % 50) == 0:
#             sys.stdout.write('.')   # show progress in terminal
#
#         i += 1
#         if not isinstance(content, str):
#             continue
#
#         try:
#             soup = BeautifulSoup(content, 'html.parser')
#             content = soup.get_text()
#             # print("preprocessing using %r" % t_args)
#             content = preprocess_text(content, **t_args)
#         except Exception:
#             logger.warning("Got an error in extracting content: %r", content)
#             import pdb; pdb.set_trace()
#             continue
#
#         # we are only interested in high level concepts
#         if kw.get('optimize_phrases'):
#             content = ' '.join(_tokenize_phrases(content))
#
#         yield content
#
#     print("\n")     # attempt to clear the sys.out
