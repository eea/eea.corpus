from colander import Schema
from eea.corpus.processing import pipeline_component
from eea.corpus.utils import set_text
import logging

logger = logging.getLogger('eea.corpus')

class Tokenizer(Schema):
    """ Schema for the Tokenizer processing.
    """

    description = "Simple, dumb tokenizer. Strips non-alpha and small words"


@pipeline_component(schema=Tokenizer,
                    title="Simple text tokenization")
def process(content, env, **settings):
    """ Tokenization
    """

    for doc in content:
        text = " ".join(tokenizer(doc.text))

        try:
            yield set_text(doc, text)
        except Exception:
            logger.exception("Error in converting to Doc %r", text)
            continue
