""" Build a list of collocations and show them in the stream.
"""

from colander import Schema
from eea.corpus.processing import pipeline_component  # , needs_tokenized_input
from eea.corpus.utils import corpus_base_path
from eea.corpus.utils import hashed_id
from gensim.models.phrases import Phrases
from itertools import tee, chain
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

    # calculate the hash id
    salt = [(env['file_name'], env['text_column'])]
    for name, settings in env['pipeline'][:env['position']+1]:
        if isinstance(settings, dict):
            settings = sorted(settings.items())
        salt.append((name, settings))
    hid = hashed_id(salt)
    cache_path = os.path.join(corpus_base_path(env['file_name']),
                              '%s.phras' % hid)
    content = (isinstance(doc, str) and Doc(doc) or doc for doc in content)
    content = (doc for doc in content if doc.lang == 'en')

    if os.path.exists(cache_path):
        phrases = Phrases()
        phrases = phrases.load(cache_path)
        cs = content
    else:
        cs, ps = tee(content, 2)
        ps = chain.from_iterable(doc.tokenized_text for doc in ps)
        phrases = Phrases(ps)     # TODO: pass settings here
        phrases.save(cache_path)

    for doc in cs:
        for sentence in phrases[doc.tokenized_text]:
            yield sentence
