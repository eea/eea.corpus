from colander import Schema, SchemaNode, Bool
from eea.corpus.processing import pipeline_component
from eea.corpus.utils import set_text
from textacy.preprocess import preprocess_text
import logging

logger = logging.getLogger('eea.corpus')


class TextacyPreprocess(Schema):
    """ Schema for textacy preprocessing pipeline component
    """

    fix_unicode = SchemaNode(
        Bool(),
        default=True,
        title="Fix Unicode",
        label='Fix broken unicode such as mojibake and garbled HTML entities.',
    )
    lowercase = SchemaNode(
        Bool(),
        default=False,
        missing=False,
        label="All text is lower-cased",
        title='Lowercase',
    )
    transliterate = SchemaNode(
        Bool(),
        default=True,
        missing=False,
        title='Transliterate',
        label="Convert non-ASCII characters to their closest ASCII equivalent."
    )
    no_urls = SchemaNode(
        Bool(),
        default=True,
        missing=False,
        title='No URLs',
        label="Replace all URL strings with ‘URL‘."
    )
    no_emails = SchemaNode(
        Bool(),
        default=True,
        missing=False,
        title='No emails',
        label="Replace all email strings with ‘EMAIL‘."
    )
    no_phone_numbers = SchemaNode(
        Bool(),
        default=True,
        missing=False,
        title='No phone numbers',
        label="Replace all phone number strings with ‘PHONE‘."
    )
    no_numbers = SchemaNode(
        Bool(),
        default=True,
        missing=False,
        title="No numbers",
        label="Replace all number-like strings with ‘NUMBER‘."
    )
    no_currency_symbols = SchemaNode(
        Bool(),
        default=True,
        missing=False,
        title="No currency symbols",
        label="Replace all currency symbols with their standard 3-letter "
        "abbreviations."
    )
    no_punct = SchemaNode(
        Bool(),
        default=True,
        missing=False,
        title="No punctuation",
        label="Remove all punctuation (replace with empty string)."
    )
    no_contractions = SchemaNode(
        Bool(),
        default=True,
        missing=False,
        title="No contractions",
        label="Replace English contractions with their unshortened forms."
    )
    no_accents = SchemaNode(
        Bool(),
        default=True,
        missing=False,
        title="No accents",
        label="Replace all accented characters with unaccented versions; "
        "NB: if transliterate is True, this option is redundant."
    )


@pipeline_component(
    schema=TextacyPreprocess,
    title="Textacy Preprocessing"
)
def process(content, env, **settings):
    for doc in content:
        try:
            text = doc.text
            text = preprocess_text(text, **settings)
        except Exception:
            logger.exception(
                "Textacy Processor: got an error in extracting content: %r",
                doc
            )
            continue
        yield set_text(doc, text)
