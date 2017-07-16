""" Pyramid views. Main UI for the eea.corpus
"""

# from deform.field import Field
from collections import namedtuple, OrderedDict
from deform import Button
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
from peppercorn import parse
from pyramid.httpexceptions import HTTPFound
from pyramid.renderers import render
from pyramid.view import view_config
from pyramid_deform import FormView
import colander as c
import deform
import hashlib
import logging
import pyramid.httpexceptions as exc
import random
import string
import sys
import traceback as tb

logger = logging.getLogger('eea.corpus')
# HTTPFound("/process/%s/" % doc)


def _resolve(field, request, appstruct):
    if field.name in request.params:
        val = request.params[field.name]
        if val is None:
            val = c.null
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


def rand(n):
    return ''.join(random.sample(string.ascii_uppercase + string.digits, k=n))


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
        # TODO: this is not safe to use
        # use pstruct = parse(self.request.POST.items())
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


class DemoSchema(c.Schema):
    """ Process text schema
    """

    title = c.SchemaNode(
        c.String(),
        validator=c.Length(min=1),
        title='Corpus title.',
        description='Letters, numbers and spaces',
    )


# TODO: replace with venusian registry or a named utility
_pipelines_registry = OrderedDict()

Pipeline = namedtuple('Pipeline', ['name', 'klass', 'title'])


def register_pipeline_schema(title):

    def wrapper(cls):
        pipeline_name = cls.__name__

        class WrappedSchema(cls):
            schema_type = c.SchemaNode(
                c.String(),
                # widget=deform.widget.HiddenWidget(),
                default=pipeline_name,
                missing=pipeline_name,
            )

        p = Pipeline(pipeline_name, WrappedSchema, title)
        _pipelines_registry[pipeline_name] = p
        return WrappedSchema

    return wrapper


@register_pipeline_schema(title="Just a Second")
class SecondSchema(c.Schema):
    """ Process text schema
    """

    second_field = c.SchemaNode(
        c.String(),
        validator=c.Length(min=1),
        title='Second title.',
    )


@register_pipeline_schema(title="Just a Third")
class ThirdSchema(c.Schema):
    """ Process text schema
    """

    third_field = c.SchemaNode(
        c.String(),
        validator=c.Length(min=1),
        title='Third title.',
    )


@view_config(
    route_name="demo",
    renderer="templates/simpleform.pt"
)
class Demo(FormView):

    def __init__(self, request):
        self.schema = DemoSchema()
        return FormView.__init__(self, request)

    def _success_handler(self, appstruct):
        print("Success generic", appstruct)

    def __getattr__(self, name):
        if name.endswith("_success") and name != 'generate_corpus_success':
            return self._success_handler

        return self.__getattribute__(name)

    @property
    def buttons(self):
        _b = [
            Button('add_%s' % x.name, 'Add %s pipeline' % x.title)
            for x in _pipelines_registry.values()
        ]
        return _b + [
            Button('generate_corpus', 'Generate Corpus'),
        ]

    def _repopulate_schema(self, schema):
        data = parse(self.request.POST.items())
        print('Repopulate schema', data)

        # recreate existing schemas.
        for k, v in data.items():
            if isinstance(v, dict):   # might be a schema cstruct
                if v.get('schema_type'):        # yeap, a schema
                    p = _pipelines_registry[v['schema_type']]
                    s = p.klass(name=k, title=p.title)
                    schema.add(s)

    def form_class(self, schema, **kwargs):
        self._repopulate_schema(schema)
        self.form = deform.Form(schema, **kwargs)
        return self.form

    def appstruct(self):
        # This is only called on success, to populate the success form

        pstruct = parse(self.request.POST.items())
        if pstruct:
            state = self.schema.deserialize(pstruct)
            return state

        return {}

    def generate_corpus_success(self, appstruct):
        # self._repopulate_schema(self.form.schema)
        print('success', appstruct)

    def show(self, form):
        # Override to recreate the form, if needed to add new schemas

        appstruct = self.appstruct()

        schema = form.schema

        # now add new schemas, at the end of all others
        data = parse(self.request.POST.items())
        for p in _pipelines_registry.values():
            if 'add_%s' % p.name in data:
                s = p.klass(name=rand(10), title=p.title)
                schema.add(s)

        use_ajax = getattr(self, 'use_ajax', False)
        ajax_options = getattr(self, 'ajax_options', '{}')
        form = deform.Form(schema, buttons=self.buttons,
                           use_ajax=use_ajax, ajax_options=ajax_options,
                           **dict(self.form_options))

        if appstruct is None:
            rendered = form.render()
        else:
            rendered = form.render(appstruct)

        return {
            'form': rendered,
        }
