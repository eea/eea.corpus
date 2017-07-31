""" Build a list of collocations and show them in the stream.
"""

from colander import Schema
from eea.corpus.async import queue
from eea.corpus.processing import build_pipeline
from eea.corpus.processing import pipeline_component  # , needs_tokenized_input
from eea.corpus.utils import corpus_base_path
from eea.corpus.utils import hashed_id
from gensim.models.phrases import Phrases
from itertools import tee, chain
from rq.decorators import job
from textacy.doc import Doc
import logging
import os.path


logger = logging.getLogger('eea.corpus')


class PhraseFinder(Schema):
    """ Schema for the phrases finder
    """

    description = "Find and process phrases in text."


@pipeline_component(schema=PhraseFinder,
                    title="Find and process phrases")
def process(content, env, **settings):       # pipeline, preview_mode,
    """ We can run in several modes:

    * remove all text but leave the phrases tokens
    * append the collocations to the document
    * replace the collocations where they appear

    TODO: implement the above
    """

    pipeline = env['pipeline'][:env['position']+1]
    file_name = env['file_name']
    text_column = env['text_column']

    hid = phrase_model_id(file_name, text_column, pipeline)

    content = (isinstance(doc, str) and Doc(doc) or doc for doc in content)
    content = (doc for doc in content if doc.lang == 'en')

    base_path = corpus_base_path(env['file_name'])
    cache_path = os.path.join(base_path, '%s.phras' % hid)

    if os.path.exists(cache_path):
        phrases = Phrases()
        phrases = phrases.load(cache_path)
        cs = content
    else:
        if env['preview_mode']:
            # if running in preview mode, look for an async job already doing
            # phrase model building
            import pdb; pdb.set_trace()
            job = queue.enqueue(build_phrases,
                                timeout='1h',
                                args=(
                                    pipeline,
                                    hid,
                                    file_name,
                                    text_column,
                                ),
                                kwargs={})
            print(job.id)
        else:
            cs, ps = tee(content, 2)
            ps = chain.from_iterable(doc.tokenized_text for doc in ps)
            phrases = Phrases(ps)     # TODO: pass settings here
            phrases.save(cache_path)

    for doc in cs:
        for sentence in phrases[doc.tokenized_text]:
            yield sentence


@job(queue=queue)
def build_phrases(pipeline, corpus_id, file_name, text_column, **kw):
    """ Async job to build a corpus using the provided pipeline
    """

    cpath = corpus_base_path(file_name)      # corpus_id
    logger.info('Creating corpus for %s at %s', file_name, cpath)

    content_stream = build_pipeline(file_name, text_column, pipeline,
                                    preview_mode=False)

    hid = phrase_model_id(file_name, text_column, pipeline)

    base_path = corpus_base_path(file_name)
    cache_path = os.path.join(base_path, '%s.phras' % hid)

    phrases = Phrases(content_stream)     # TODO: pass settings here
    phrases.save(cache_path)


def phrase_model_id(file_name, text_column, pipeline):
    # calculate the hash id
    salt = [(file_name, text_column)]
    for name, settings in pipeline:
        if isinstance(settings, dict):
            settings = sorted(settings.items())
        salt.append((name, settings))
    return hashed_id(salt)
