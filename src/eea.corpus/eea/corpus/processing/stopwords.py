""" Remove stop words from text
"""

import logging

from colander import Schema

import nltk
from eea.corpus.processing import pipeline_component  # , needs_text_input
from eea.corpus.utils import set_text
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize

# from spacy.lang.en import English
# from spacy.tokenizer import Tokenizer
# nlp = English()
# tokenizer = Tokenizer(nlp.vocab)

logger = logging.getLogger('eea.corpus')

try:
    dl = nltk.downloader.Downloader()

    if not dl.is_installed('stopwords'):
        nltk.download('stopwords')      # TODO: do this some other way

    if not dl.is_installed('punkt'):
        nltk.download('punkt')          # TODO: do this some other way
except Exception:
    logger.exception("Error when checking for nltk's stopwords data")


class StopWords(Schema):
    """ Schema for BeautifulSoup based parser
    """
    description = "Filter our common English stopwords"


@pipeline_component(schema=StopWords,
                    title="Remove stop words")
def process(content, env, **settings):
    stops = stopwords.words('english')

    for doc in content:

        words = word_tokenize(doc['text'])
        text = [w for w in words if w not in stops]
        text = " ".join(text)

        yield set_text(doc, text)
