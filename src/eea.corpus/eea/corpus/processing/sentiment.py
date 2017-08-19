""" Get the text based on sentiment score
"""

from colander import Schema, SchemaNode, Float
from eea.corpus.processing import pipeline_component    # , needs_text_input
from eea.corpus.utils import to_doc
import logging

logger = logging.getLogger('eea.corpus')


class Sentiment(Schema):
    """ Schema for Sentiment filter
    """
    description = "Filter documents based on their sentiment value"

    threshold = SchemaNode(
        Float(),
        default=0.5,
        missing=0.5,
        title='Results limit',
        description='Set to 0 if you want unlimited results',
    )


@pipeline_component(schema=Sentiment,
                    title="WIP - Sentiment-based filter")
def process(content, env, **settings):
    tr = settings['threshold']
    content = (to_doc(doc) for doc in content)
    for doc in content:
        st = doc.spacy_doc.sentiment
        if st > tr:
            yield doc
