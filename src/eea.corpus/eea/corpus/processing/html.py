""" Get the text from potential html strings using bs4
"""

from bs4 import BeautifulSoup
from colander import Schema
from eea.corpus.processing import pipeline_component
import logging

logger = logging.getLogger('eea.corpus')


class BeautifulSoupText(Schema):
    """ Schema for BeautifulSoup based parser
    """
    description = "Strips HTML tags, leaving only plain text."


@pipeline_component(schema=BeautifulSoupText,
                    title="Strip tags with BeautifulSoup")
def process(content, **settings):
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
