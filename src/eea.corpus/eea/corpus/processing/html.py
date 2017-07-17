""" Get the text from potential html strings using bs4
"""

from bs4 import BeautifulSoup
from colander import Schema
from eea.corpus.processing import register_pipeline_component
import logging

logger = logging.getLogger('eea.corpus')


class BeautifulSoupText(Schema):
    """ Schema for BeautifulSoup based parser
    """


def process(content, **settings):
    for doc in content:
        try:
            soup = BeautifulSoup(doc, 'html.parser')
            clean = soup.get_text()
        except Exception:
            logger.warning(
                "BS4 Processor: got an error in extracting content: %r",
                doc
            )
            continue

        yield clean


def includeme(config):
    register_pipeline_component(
        name="bs4_get_text",
        schema=BeautifulSoupText,
        process=process,
        title="Strip tags with BeautifulSoup"
    )
