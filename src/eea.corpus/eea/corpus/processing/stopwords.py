""" Remove stop words from text
"""

from colander import Schema
from eea.corpus.processing import pipeline_component    # , needs_text_input
from eea.corpus.utils import set_text
from nltk.corpus import stopwords
import logging
import nltk

logger = logging.getLogger('eea.corpus')

dl = nltk.downloader.Downloader()
if not dl.is_installed('stopwords'):
    nltk.download('stopwords')      # TODO: do this some other way


class StopWords(Schema):
    """ Schema for BeautifulSoup based parser
    """
    description = "Filter our common English stopwords"


@pipeline_component(schema=StopWords,
                    title="Remove stop words")
def process(content, env, **settings):
    stops = stopwords.words('english')

    for doc in content:

        text = [
            [w for w in sent if w not in stops] for sent in doc.tokenized_text
        ]
        text = ". ".join(" ".join(sent) for sent in text)

        yield set_text(doc, text)
