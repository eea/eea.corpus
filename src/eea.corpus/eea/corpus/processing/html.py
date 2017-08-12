""" Get the text from potential html strings using bs4
"""

from bs4 import BeautifulSoup
from colander import Schema
from eea.corpus.processing import pipeline_component    # , needs_text_input
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
        try:
            soup = BeautifulSoup(doc, 'html.parser')
            clean = soup.get_text()
        except Exception:
            logger.exception(
                "BS4 Processor: got an error in extracting content: %r",
                doc
            )
            continue

        yield clean
