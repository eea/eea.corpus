import colander
import deform.widget
from colander import Schema

from eea.corpus.processing.phrases.widget import PhraseFinderWidget


class PhraseFinder(Schema):
    """ Schema for the phrases finder
    """

    widget = PhraseFinderWidget()       # overrides the default template
    description = "Find and process phrases in text."

    MODES = (
        ('tokenize', 'Tokenize phrases in text'),
        ('append', 'Append phrases to text'),
        ('replace', 'Replace all text with found phrases')
    )

    SCORING = (
        ('default', 'Default'),
        ('npmi', 'NPMI: Slower, better with common words'),
    )

    LEVELS = (
        (2, 'Bigrams'),
        (3, 'Trigrams'),
        (4, 'Quadgrams'),
    )

    mode = colander.SchemaNode(
        colander.String(),
        validator=colander.OneOf([x[0] for x in MODES]),
        default=MODES[0][0],
        missing=MODES[0][0],
        title="Operating mode",
        widget=deform.widget.RadioChoiceWidget(values=MODES)
    )

    level = colander.SchemaNode(
        colander.Int(),
        default=LEVELS[0][0],
        missing=LEVELS[0][0],
        title='N-gram level',
        widget=deform.widget.RadioChoiceWidget(values=LEVELS),
        description='How many words to include in phrase detection',
    )

    min_count = colander.SchemaNode(
        colander.Int(),
        default=5,
        missing=5,
        title='Minimum number',
        description='Ignore all words with total count lower then this',
    )

    threshold = colander.SchemaNode(
        colander.Float(),
        default=10.0,
        missing=10.0,
        title='Threshold',
        description='Score threshold for forming phrases. Higher means '
                    'fewer phrases.',
    )

    scoring = colander.SchemaNode(
        colander.String(),
        validator=colander.OneOf([x[0] for x in SCORING]),
        default=SCORING[0][0],
        missing=SCORING[0][0],
        title="Scoring algorithm",
        widget=deform.widget.RadioChoiceWidget(values=SCORING)
    )
