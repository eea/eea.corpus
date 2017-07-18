from colander import Int, Schema, SchemaNode, String, Float, Bool
from eea.corpus.processing import pipeline_registry
from eea.corpus.utils import upload_location
import colander
import deform
import pandas as pd


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


class PipelineSelectWidget(deform.widget.SelectWidget):
    template = "pipeline_select"


@colander.deferred
def pipeline_components_select_widget(node, kw):
    """ A select widget that reads the csv file to show available columns
    """

    choices = [(p.name, p.title) for p in pipeline_registry.values()]
    default = ''
    return PipelineSelectWidget(
        values=choices,
        default=default
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

    pipeline_components = SchemaNode(
        String(),
        widget=pipeline_components_select_widget,
        title="Add a new pipeline component"
    )
