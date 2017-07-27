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
def process(content, **settings):
    """ We can run in several modes:

    * remove all text but leave the phrases tokens
    * append the collocations to the document
    * replace the collocations where they appear

    """

    # from itertools import tee
    # cs, ps = tee(content, 2)

    phrases = Phrases()     # TODO: pass settings here
    i = 0
    for doc in content:
        if isinstance(doc, str):
            doc = Doc(doc)

        if doc.lang != 'en':
            continue

        print(i)
        i += 1
        phrases.learn_vocab(doc.tokenized_text, 40000000)

    import pdb; pdb.set_trace()
    for doc in cs:
        yield doc
