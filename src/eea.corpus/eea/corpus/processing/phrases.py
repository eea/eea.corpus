""" Build a list of collocations and show them in the stream.
"""

from colander import Schema
from eea.corpus.processing import pipeline_component  # , needs_tokenized_input
from gensim.models.phrases import Phrases
from textacy.doc import Doc
import logging

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

    from itertools import tee, chain

    content = (isinstance(doc, str) and Doc(doc) or doc for doc in content)
    content = (doc for doc in content if doc.lang == 'en')

    cs, ps = tee(content, 2)

    ps = chain.from_iterable(doc.tokenized_text for doc in ps)

    phrases = Phrases(ps)     # TODO: pass settings here

    # phrases.learn_vocab(doc.tokenized_text, 40000000)

    # import pdb; pdb.set_trace()
    for doc in cs:
        for sentence in phrases[doc.tokenized_text]:
            yield sentence
