""" Get the text from potential html strings using bs4
"""

from bs4 import BeautifulSoup
from colander import Schema
from eea.corpus.processing import pipeline_component    # , needs_text_input
from eea.corpus.utils import set_text
import logging

logger = logging.getLogger('eea.corpus')


class BeautifulSoupText(Schema):
    """ Schema for BeautifulSoup based parser
    """
    description = "Uses BeautifulSoup to extract plain text from HTML content."


@pipeline_component(schema=BeautifulSoupText,
                    title="Strip HTML tags")
def process(content, env, **settings):
    for doc in content:
        text = doc.text
        try:
            soup = BeautifulSoup(text, 'html.parser')
            clean = soup.get_text()
        except Exception:
            logger.exception(
                "BS4 Processor: got an error in extracting content: %r",
                doc
            )
            continue

        try:
            yield set_text(doc, clean)
        except Exception:
            logger.exception(
                "BS4 Processor: got an error converting to Doc: %r",
                doc
            )
            continue
