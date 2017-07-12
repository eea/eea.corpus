""" Pyramid views. Main UI for the eea.corpus
"""

from eea.corpus.async import queue
from eea.corpus.corpus import build_corpus
from eea.corpus.schema import CreateCorpusSchema
from eea.corpus.schema import TopicExtractionSchema
from eea.corpus.schema import UploadSchema
from eea.corpus.topics import pyldavis_visualization
from eea.corpus.topics import termite_visualization
from eea.corpus.topics import wordcloud_visualization
from eea.corpus.utils import available_documents
from eea.corpus.utils import delete_corpus
from eea.corpus.utils import document_name
from eea.corpus.utils import extract_corpus_id
from eea.corpus.utils import get_corpus
from eea.corpus.utils import metadata
from eea.corpus.utils import upload_location
from pyramid.httpexceptions import HTTPFound
from pyramid.renderers import render
from pyramid.view import view_config
from pyramid_deform import FormView
import colander
import hashlib
import logging
import pyramid.httpexceptions as exc
import sys
import traceback as tb

logger = logging.getLogger('eea.corpus')
# HTTPFound("/process/%s/" % doc)


def _resolve(field, request, appstruct):
    if field.name in request.params:
        val = request.params[field.name]
        if val is None:
            val = colander.null
        appstruct[field.name] = val
    for child in field.children:
        _resolve(child, request, appstruct)


def get_appstruct(request, schema):
    """ Function inspired by similar code from Kotti
    """
    appstruct = {}
    for field in schema.children:
        _resolve(field, request, appstruct)
    return appstruct


@view_config(route_name='home', renderer='templates/home.pt')
def home(request):
    documents = available_documents(request)
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
    route_name="view_corpus",
    renderer="templates/topics.pt"
)
class TopicsView(FormView):
    schema = TopicExtractionSchema()
    buttons = ('view', 'termite', 'wordcloud')

    vis = None

    def corpus(self):
        """ Return a corpus based on environment.

        It will try to return it from cache, otherwise load it from disk.
        If corpus hasn't been extracted from the document, it will redirect to
        a corpus creation tool.
        """

        corpus = get_corpus(self.request)
        if corpus is None:
            raise exc.HTTPNotFound()
        return corpus

    def metadata(self):
        """ Show metadata about context document
        """
        # TODO: show info about processing and column
        return metadata(self.corpus())

    def before(self, form):
        form.appstruct = self.appstruct()

    def appstruct(self):
        appstruct = get_appstruct(self.request, self.schema)
        # fname = document_name(self.request)
        # appstruct['column'] = default_column(fname, self.request)

        return appstruct

    def visualise(self, appstruct, method):
        max_df = appstruct['max_df']
        min_df = appstruct['min_df']
        mds = appstruct['mds']
        num_docs = appstruct['num_docs']
        topics = appstruct['topics']

        corpus = self.corpus()
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
class CreateCorpusView(FormView):
    schema = CreateCorpusSchema()
    buttons = ('generate corpus', )

    @property
    def document(self):
        return document_name(self.request)

    def before(self, form):
        form.appstruct = self.appstruct()

    def appstruct(self):
        appstruct = get_appstruct(self.request, self.schema)
        # fname = document_name(self.request)
        # appstruct['column'] = default_column(fname, self.request)
        return appstruct

    def generate_corpus_id(self, appstruct):
        m = hashlib.sha224()
        for kv in sorted(appstruct.items()):
            m.update(str(kv).encode('ascii'))
        return m.hexdigest()

    def generate_corpus_success(self, appstruct):
        print(appstruct)

        s = appstruct.copy()
        s['doc'] = self.document
        corpus_id = self.generate_corpus_id(s)

        job = queue.enqueue(build_corpus,
                            timeout='1h',
                            args=(corpus_id,
                                  self.document,
                                  appstruct['column']),
                            kwargs=appstruct)

        raise exc.HTTPFound('/view/%s/%s/job/%s' %
                            (self.document, corpus_id, job.id))

        # import pdb; pdb.set_trace()
        # cache = self.request.corpus_cache
        # cache[self.document] = {
        #     corpus_name: corpus
        # }
        # raise exc.HTTPFound('/view/%s/%s' % (self.document, corpus_name))


@view_config(route_name='view_job', renderer='templates/job.pt')
def view_job(request):
    jobid = request.matchdict.get('job')
    job = queue.fetch_job(jobid)
    return {'job': job}


@view_config(route_name='delete_corpus')
def delete_corpus_view(request):
    doc, corpus = extract_corpus_id(request)
    delete_corpus(doc, corpus)
    request.session.flash("Corpus deleted")
    raise exc.HTTPFound('/')


@view_config(context=Exception, renderer='templates/error.pt')
def handle_exc(context, request):
    _type, value, tr = sys.exc_info()
    error = " ".join(tb.format_exception(_type, value, tr))
    logger.error(error)
    return {
        'error': error
    }
