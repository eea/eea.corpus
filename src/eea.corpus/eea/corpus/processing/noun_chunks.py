import logging

import colander
import deform.widget

from eea.corpus.processing import pipeline_component
from eea.corpus.utils import set_text, tokenize
from textacy.doc import Doc
from textacy.extract import noun_chunks

logger = logging.getLogger('eea.corpus')


class NounChunks(colander.Schema):
    """ Schema for the NounChunks processing.
    """

    description = "Find and process noun chunks in text."

    MODES = (
        ('tokenize', 'Tokenize noun chunks in text'),
        ('append', 'Add tokenized noun chunks to text'),
        ('replace', 'Replace all text with found tokenized noun chunks')
    )

    mode = colander.SchemaNode(
        colander.String(),
        validator=colander.OneOf([x[0] for x in MODES]),
        default=MODES[0][0],
        missing=MODES[0][0],
        title="Operating mode",
        widget=deform.widget.RadioChoiceWidget(values=MODES)
    )

    drop_determiners = colander.SchemaNode(
        colander.Bool(),
        default=True,
        missing=False,
        label="Determiners are leading word particles (ex: the)",
        title='Drop determiners',
    )

    min_freq = colander.SchemaNode(
        colander.Int(),
        title="Minimum frequency count",
        description="""Ignore phrases with lower count then given number""",
        default=1,
    )


@pipeline_component(schema=NounChunks,
                    title="Find and process noun chunks")
def process(content, env, **settings):
    """ Noun Chunks processing
    """

    mode = settings.get('mode', 'tokenize')

    drop_deter = settings['drop_determiners']
    min_freq = int(settings['min_freq'])

    for doc in content:
        text = doc['text']

        try:
            td = Doc(text)
            ncs = [x.text for x in noun_chunks(td,
                                               drop_determiners=drop_deter,
                                               min_freq=min_freq)]
        except Exception:
            logger.exception("Error extracting noun chunks %r", doc)

            continue

        if mode == 'tokenize':
            for nc in ncs:
                text = text.replace(nc, tokenize(nc))

        if mode == 'append':
            text = ' '.join([text] + [tokenize(nc) for nc in ncs])

        if mode == 'replace':
            text = ' '.join([tokenize(nc) for nc in ncs])

        try:
            yield set_text(doc, text)
        except Exception:
            logger.exception("Error in converting to Doc %r", text)

            continue
