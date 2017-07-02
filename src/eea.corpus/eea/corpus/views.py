from eea.corpus.corpus import build_corpus
from eea.corpus.corpus import load_corpus
from eea.corpus.schema import ProcessSchema
from eea.corpus.schema import TopicExtractionSchema
from eea.corpus.schema import UploadSchema
from eea.corpus.topics import pyldavis_visualization
from eea.corpus.topics import termite_visualization
from eea.corpus.topics import wordcloud_visualization
from eea.corpus.utils import available_documents
from eea.corpus.utils import default_column
from eea.corpus.utils import document_name
from eea.corpus.utils import upload_location
from pyramid.httpexceptions import HTTPFound
from pyramid.renderers import render
from pyramid.view import view_config
from pyramid_deform import FormView
import colander
import pyramid.httpexceptions as exc


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
    documents = available_documents()
    return {
        'project': 'EEA Corpus Server',
        'documents': documents
    }


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


@view_config(
    route_name="view_csv",
    renderer="templates/topics.pt"
)
class TopicsView(FormView):
    schema = TopicExtractionSchema()
    buttons = ('view', 'termite', 'wordcloud')

    vis = None

    def corpus(self, text_column="text"):
        """ Return a corpus based on environment.

        It will try to return it from cache, otherwise load it from disk.
        If corpus hasn't been extracted from the document, it will extract it
        now.
        """

        cache = self.request.corpus_cache
        fname = document_name(self.request)
        if (fname not in cache) or (text_column not in cache[fname]):
            corpus = load_corpus(file_name=fname, text_column=text_column)

            if corpus is None:
                raise exc.HTTPFound("/process/%s/" % fname)

            cache[fname] = {
                text_column: corpus
            }

        return cache[fname][text_column]

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

    def before(self, form):
        form.appstruct = self.appstruct()

    def appstruct(self):
        appstruct = get_appstruct(self.request, self.schema)
        fname = document_name(self.request)
        appstruct['column'] = default_column(fname, self.request)

        return appstruct

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


@view_config(
    route_name="process_csv",
    renderer="templates/process.pt"
)
class ProcessView(FormView):
    schema = ProcessSchema()
    buttons = ('generate corpus', )

    vis = None

    @property
    def document(self):
        return document_name(self.request)

    def before(self, form):
        form.appstruct = self.appstruct()

    def appstruct(self):
        appstruct = get_appstruct(self.request, self.schema)
        fname = document_name(self.request)
        appstruct['column'] = default_column(fname, self.request)
        return appstruct

    def generate_corpus_success(self, appstruct):
        print(appstruct)
        text_column = appstruct.pop('column')
        corpus = build_corpus(self.document, text_column=text_column,
                              **appstruct)
        cache = self.request.corpus_cache
        cache[self.document] = {
            text_column: corpus
        }
        raise exc.HTTPFound('/view/%s/' % self.document)
