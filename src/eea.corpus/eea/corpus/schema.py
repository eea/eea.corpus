from eea.corpus.utils import upload_location
from colander import Int, Schema, SchemaNode, String, Float, Bool
import deform
import pandas as pd
import colander


class Store(dict):
    def preview_url(self, name):
        return ""


tmpstore = Store()


@colander.deferred
def columns_widget(node, kw):
    """ A select widget that reads the csv file to show available columns
    """

    choices = []
    req = kw['request']
    md = req.matchdict or {}
    name = md.get('doc')
    if name:
        path = upload_location(name)        # TODO: move this to utils
        f = pd.read_csv(path)
        choices = [('', '')] + [(k, k) for k in f.keys()]

    # from eea.corpus.utils import document_name
    # from eea.corpus.utils import default_column
    # doc = document_name(req)
    # default = default_column(file_name, req)
    default = ''
    return deform.widget.SelectWidget(
        values=choices,
        default=default
    )


class UploadSchema(Schema):
    # title = SchemaNode(String())
    upload = SchemaNode(
        deform.FileData(),
        widget=deform.widget.FileUploadWidget(tmpstore)
    )


class TopicExtractionSchema(Schema):
    topics = SchemaNode(
        Int(),
        default=10,
        title="Number of topics to extract"
    )
    num_docs = SchemaNode(
        Int(),
        default=100,
        title="Max number of documents to process"
    )
    # column = SchemaNode(
    #     String(),
    #     widget=columns_widget,
    #     title='Text column in CSV file',
    #     missing='',
    # )
    min_df = SchemaNode(
        Float(),
        title="min_df",
        description="""Ignore terms that have
        a document frequency strictly lower than the given threshold. This
        value is also called cut-off in the literature. The parameter
        represents a proportion of documents.""",
        default=0.1,
    )
    max_df = SchemaNode(
        Float(),
        title="max_df",
        description=""" Ignore terms that have
        a document frequency strictly higher than the given threshold
        (corpus-specific stop words). The parameter represents
        a proportion of documents. """,
        default=0.7,
    )
    mds = SchemaNode(
        String(),
        title="Distance scaling algorithm (not for termite plot)",
        description="Multidimensional Scaling algorithm. See "
        "https://en.wikipedia.org/wiki/Multidimensional_scaling",
        widget=deform.widget.SelectWidget(
            values=[
                ('pcoa', 'PCOA (Classic Multidimensional Scaling)'),
                ('mmds', 'MMDS (Metric Multidimensional Scaling)'),
                ('tsne',
                 't-SNE (t-distributed Stochastic Neighbor Embedding)'),
            ],
            default='pcoa'
        )
    )


class TextacyPipeline(Schema):

    fix_unicode = SchemaNode(
        Bool(),
        default=True,
        title="Fix Unicode",
        label='Fix broken unicode such as mojibake and garbled HTML entities.',
    )
    lowercase = SchemaNode(
        Bool(),
        default=False,
        label="All text is lower-cased",
        title='Lowercase',
    )
    transliterate = SchemaNode(
        Bool(),
        default=True,
        title='Transliterate',
        label="Convert non-ASCII characters to their closest ASCII equivalent."
    )
    no_urls = SchemaNode(
        Bool(),
        default=True,
        title='No URLs',
        label="Replace all URL strings with ‘URL‘."
    )
    no_emails = SchemaNode(
        Bool(),
        default=True,
        title='No emails',
        label="Replace all email strings with ‘EMAIL‘."
    )
    no_phone_numbers = SchemaNode(
        Bool(),
        default=True,
        title='No phone numbers',
        label="Replace all phone number strings with ‘PHONE‘."
    )
    no_numbers = SchemaNode(
        Bool(),
        default=True,
        title="No numbers",
        label="Replace all number-like strings with ‘NUMBER‘."
    )
    no_currency_symbols = SchemaNode(
        Bool(),
        default=True,
        title="No currency symbols",
        label="Replace all currency symbols with their standard 3-letter "
        "abbreviations."
    )
    no_punct = SchemaNode(
        Bool(),
        default=True,
        title="No punctuation",
        label="Remove all punctuation (replace with empty string)."
    )
    no_contractions = SchemaNode(
        Bool(),
        default=True,
        title="No contractions",
        label="Replace English contractions with their unshortened forms."
    )
    no_accents = SchemaNode(
        Bool(),
        default=True,
        title="No accents",
        label="Replace all accented characters with unaccented versions; "
        "NB: if transliterate is True, this option is redundant."
    )


class CreateCorpusSchema(colander.MappingSchema):
    """ Process text schema
    """

    title = SchemaNode(
        String(),
        validator=colander.Length(min=1),
        title='Corpus title.',
        description='Letters, numbers and spaces',
    )
    description = SchemaNode(
        String(),
        widget=deform.widget.TextAreaWidget(),
        title='Description',
        missing='',
    )

    column = SchemaNode(
        String(),
        widget=columns_widget,
        validator=colander.Length(min=1),
        title='Text column in CSV file',
    )
    normalize = SchemaNode(
        Bool(),
        default=True,
        title="Enable text normalization",
        label='Preprocess the text according to settings below. '
        'All other settings are ignored.',
    )

    textacy_pipeline = TextacyPipeline()
