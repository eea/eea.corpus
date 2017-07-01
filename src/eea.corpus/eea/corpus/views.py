from colander import Int, Schema, SchemaNode, String, Float
from eea.corpus.topics import pyldavis_visualization
from eea.corpus.topics import termite_visualization
from eea.corpus.topics import wordcloud_visualization
from eea.corpus.utils import available_columns
from eea.corpus.utils import load_or_create_corpus, available_files
from eea.corpus.utils import upload_location, is_valid_document
from pyramid.httpexceptions import HTTPFound
from pyramid.renderers import render
from pyramid.view import view_config
from pyramid_deform import FormView
import colander
import deform
import pandas as pd


class Store(dict):
    def preview_url(self, name):
        return ""


tmpstore = Store()
CACHE = {}      # really dummy and simple way to cache corpuses


def get_appstruct(request, schema):
    """ Function inspired by similar code from Kotti
    """
    appstruct = {}
    for field in schema.children:
        if field.name in request.params:
            val = request.params[field.name]
            if val is None:
                val = colander.null
            appstruct[field.name] = val
    return appstruct


@view_config(route_name='home', renderer='templates/home.pt')
def home(request):
    files = available_files()
    return {'project': 'EEA Corpus Server', 'filelist': files}


class UploadSchema(Schema):
    # title = SchemaNode(String())
    upload = SchemaNode(
        deform.FileData(),
        widget=deform.widget.FileUploadWidget(tmpstore)
    )


@view_config(
    route_name="upload_csv",
    renderer="templates/simpleform.pt"
)
class UploadView(FormView):
    schema = UploadSchema()
    buttons = ('save',)

    def save_success(self, appstruct):
        upload = appstruct.get('upload')
        if upload:
            fname = upload['filename']
            path = upload_location(fname)
            with open(path, 'wb') as f:
                f.write(upload['fp'].read())

        self.request.session.flash(u"Your changes have been saved.")
        return HTTPFound(location='/')


def default_column(file_name, request):
    """ Identify the "default" column.

    * If a given column name is given in request, use that.
    * if not, identify it the corpus folder has any folders for columns.
        Use the first available such column
    """
    column = request.params.get('column') or ''

    # if there's no column, try to identify a column from the cache
    if not column:
        columns = list(CACHE.get(file_name, {}))
        if columns:
            column = columns[0]     # grab the first cached

    # if there's no column, try to identify a column from the var dir
    columns = available_columns(file_name)
    column = columns and columns[0] or 'text'
    return column


def document_name(request):
    """ Extract document name (aka file_name) from request
    """

    md = request.matchdict or {}
    fname = md.get('name')
    return is_valid_document(fname) and fname


@colander.deferred
def columns_widget(node, kw):
    """ A select widget that reads the csv file to show available columns
    """

    choices = []
    req = kw['request']
    md = req.matchdict or {}
    name = md.get('name')
    if name:
        path = upload_location(name)        # TODO: move this to utils
        f = pd.read_csv(path)
        choices = [('', '')] + [(k, k) for k in f.keys()]

    file_name = document_name(req)
    default = default_column(file_name, req)
    return deform.widget.SelectWidget(values=choices, default=default)


class TopicsSchema(Schema):
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
    column = SchemaNode(
        String(),
        widget=columns_widget,
        title='Text column in CSV file',
        missing='',
    )
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


@view_config(
    route_name="view_csv",
    renderer="templates/topics.pt"
)
class TopicsView(FormView):
    schema = TopicsSchema()
    buttons = ('view', 'termite', 'wordcloud')

    vis = None

    def corpus(self, text_column="text"):
        """ Return a corpus based on environment.

        It will try to return it from cache, otherwise load it from disk.
        If corpus hasn't been extracted from the document, it will extract it
        now.
        """

        fname = document_name(self.request)
        if (fname not in CACHE) or (text_column not in CACHE[fname]):
            # fpath = os.path.join(CORPUS_PATH, fname)
            corpus = load_or_create_corpus(file_name=fname,
                                           text_column=text_column,
                                           normalize=True,
                                           optimize_phrases=False)
            CACHE[fname] = {text_column: corpus}

        return CACHE[fname][text_column]

    def metadata(self):
        """ Show metadata about context document
        """
        fname = document_name(self.request)
        corpus = self.corpus(text_column=default_column(fname, self.request))
        return {
            'docs': corpus.n_docs,
            'sentences': corpus.n_sents,
            'tokens': corpus.n_tokens,
            'lang': corpus.spacy_lang.lang,
        }

    def visualise(self, appstruct, method):
        column = appstruct['column']
        max_df = appstruct['max_df']
        min_df = appstruct['min_df']
        mds = appstruct['mds']
        num_docs = appstruct['num_docs']
        topics = appstruct['topics']

        corpus = self.corpus(text_column=column)
        MAP = {
            'pyLDAvis': pyldavis_visualization,
            'termite': termite_visualization,
            'wordcloud': wordcloud_visualization,
        }

        visualizer = MAP[method]
        vis = visualizer(corpus, topics, num_docs, min_df, max_df, mds)
        return vis

    def view_success(self, appstruct):
        self.vis = self.visualise(appstruct, method='pyLDAvis')

    def termite_success(self, appstruct):
        self.vis = self.visualise(appstruct, method='termite')

    def wordcloud_success(self, appstruct):
        topics = self.visualise(appstruct, method='wordcloud')
        out = render('templates/wordcloud_fragments.pt',
                     {'topics': topics})

        self.vis = out

    def before(self, form):
        appstruct = get_appstruct(self.request, self.schema)

        fname = document_name(self.request)
        appstruct['column'] = default_column(fname, self.request)

        form.appstruct = appstruct

    def appstruct(self):
        appstruct = get_appstruct(self.request, self.schema)
        fname = document_name(self.request)
        appstruct['column'] = default_column(fname, self.request)

        return appstruct
