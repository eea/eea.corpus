""" Build a list of collocations and show them in the stream.
"""

# from eea.corpus.processing import register_pipeline_component
from bs4 import BeautifulSoup
from colander import Schema
from eea.corpus.processing import pipeline_component
import logging

logger = logging.getLogger('eea.corpus')


class CollocationsFinder(Schema):
    """ Schema for the collocations finder
    """

    description = "Find and process collocations in text."


@pipeline_component(schema=CollocationsFinder,
                    title="Find and process collocations")
def process(content, **settings):
    """ We can run in several modes:

    * remove all text but leave the collocation tokens
    * append the collocations to the document
    * replace the collocations where they appear


    """

    import pdb; pdb.set_trace()

    # First we need to identify collocations
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
#
#
# def includeme(config):
#     register_pipeline_component(
#         schema=CollocationsFinder,
#         process=process,
#         title="Find and process collocations"
#     )
#
