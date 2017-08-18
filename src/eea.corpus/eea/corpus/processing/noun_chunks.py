from colander import Schema
from eea.corpus.processing import pipeline_component
from eea.corpus.utils import to_doc, tokenize
from textacy.doc import Doc
import colander
import deform.widget
import logging

logger = logging.getLogger('eea.corpus')


class NounChunks(Schema):
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


@pipeline_component(schema=NounChunks,
                    title="Find and process noun chunks")
def process(content, env, **settings):
    """ Noun Chunks processing
    """

    content = (to_doc(doc) for doc in content)

    mode = settings.get('mode', 'tokenize')

    for doc in content:
        try:
            ncs = [x.text for x in doc.spacy_doc.noun_chunks]
        except Exception:
            logger.exception("Error extracting noun chunks %r", doc)
            continue

        text = doc.text

        # TODO: see if able to use Doc.merge for replacements
        if mode == 'tokenize':
            for nc in ncs:
                text = text.replace(nc, tokenize(nc))
        if mode == 'append':
            text = ' '.join([text] + [tokenize(nc) for nc in ncs])
        if mode == 'replace':
            text = ' '.join([tokenize(nc) for nc in ncs])

        try:
            yield Doc(text)
        except Exception:
            logger.exception("Error in converting to Doc %r", text)
            continue
