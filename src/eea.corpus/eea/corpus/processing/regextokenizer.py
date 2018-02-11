# TODO: add missing import

import logging

import colander

from eea.corpus.processing import pipeline_component
from eea.corpus.utils import set_text

logger = logging.getLogger('eea.corpus')


class RegexTokenizer(colander.Schema):
    """ Schema for the Tokenizer processing.
    """

    description = "Simple, dumb tokenizer. Strips non-alpha and small words"

    regex = colander.SchemaNode(
        colander.String(),
        title="Regular expression",
        missing="",
        # usable for tokenizing code
        # based on http://blog.aylien.com/source-code-classification-using-deep-learning/
        default=r'[\w\']+|[""!"#$%&\'()*+,-./:;<=>?@[\]^_`{|}~""\\]',
    )


@pipeline_component(schema=RegexTokenizer,
                    title="Regex based tokenizer")
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
