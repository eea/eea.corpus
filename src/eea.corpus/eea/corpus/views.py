""" Pyramid views. Main UI for the eea.corpus
"""

from deform import Button
from deform import Form
from deform import ZPTRendererFactory
from eea.corpus.async import queue
from eea.corpus.corpus import build_corpus
from eea.corpus.processing import build_pipeline
from eea.corpus.processing import pipeline_registry
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
from pkg_resources import resource_filename
from pyramid.httpexceptions import HTTPFound
from pyramid.renderers import render
from pyramid.view import view_config
from pyramid_deform import FormView
import deform
import hashlib
import logging
import pyramid.httpexceptions as exc
import random
import string
import sys
import traceback as tb

logger = logging.getLogger('eea.corpus')

# Configure alternative Deform templates renderer. Includes overrides for
# default deform templates
deform_templates = resource_filename('deform', 'templates')
eeacorpus_templates = resource_filename('eea.corpus', 'templates/deform')
search_path = (eeacorpus_templates, deform_templates)
deform_renderer = ZPTRendererFactory(search_path)


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


def schema_defaults(schema):
    """ Returns a mapping of fielname:defaultvalue
    """
    res = {}
    for child in schema.children:
        res[child.name] = child.default or child.missing
    return res


@view_config(
    route_name="process_csv",
    renderer="templates/create_corpus.pt"
)
class CreateCorpusView(FormView):
    schema = CreateCorpusSchema()

    preview = ()        # will hold preview results
    preview_size = 5    # number of documents (csv rows) to preview

    buttons = (
        Button('preview', 'Preview'),
        Button('generate_corpus', 'Generate Corpus'),
        Button('save_pipeline', 'Save pipeline as template'),
    )

    @property
    def document(self):
        return document_name(self.request)

    def generate_corpus_id(self, appstruct):
        m = hashlib.sha224()
        for kv in sorted(appstruct.items()):
            m.update(str(kv).encode('ascii'))
        return m.hexdigest()

    def get_pipeline_components(self):
        """ Returns a pipeline, a list of (process, arguments)

        It uses the request to understand the structure of the pipeline. The
        significant elements of that structure are the pipeline component name,
        its position in the schema and its settings.
        """

        data = parse(self.request.POST.items())
        state = self.schema.deserialize(data)

        # recreate existing schemas.
        schemas = {}

        for k, v in data.items():
            if isinstance(v, dict):   # might be a schema cstruct
                _type = v.pop('schema_type', None)
                if _type is not None:       # yeap, a schema
                    p = pipeline_registry[_type]
                    kwargs = state[k].copy()
                    kwargs.pop('schema_type')
                    position = kwargs.pop('schema_position')
                    schemas[position] = (p.name, kwargs)

        return [schemas[k] for k in sorted(schemas.keys())]

    def preview_success(self, appstruct):
        # no action needed, it's done by show()
        return

    def generate_corpus_success(self, appstruct):
        pipeline = self.get_pipeline_components()

        s = appstruct.copy()
        s['doc'] = self.document
        corpus_id = self.generate_corpus_id(s)

        job = queue.enqueue(build_corpus,
                            timeout='1h',
                            args=(
                                pipeline,
                                corpus_id,
                                self.document,
                                appstruct['column'],
                            ),
                            kwargs=appstruct)

        raise exc.HTTPFound('/view/%s/%s/job/%s' %
                            (self.document, corpus_id, job.id))

    def _extract_pipeline_schemas(self):
        data = parse(self.request.POST.items())
        schemas = {}
        for k, v in data.items():
            if isinstance(v, dict):   # might be a schema cstruct
                _type = v.pop('schema_type', None)
                if _type is not None:       # yeap, a schema
                    p = pipeline_registry[_type]
                    s = p.schema(name=k, title=p.title,
                                 renderer=deform_renderer)
                    pos = v.pop('schema_position')
                    schemas[pos] = s

        # Handle subschemas clicked buttons: perform apropriate operations
        schemas = [schemas[i] for i in sorted(schemas.keys())]
        return schemas

    def form_class(self, schema, **kwargs):
        data = parse(self.request.POST.items())
        schemas = self._extract_pipeline_schemas()
        schemas = self._apply_schema_edits(schemas, data)
        for s in schemas:
            schema.add(s)

        # move the pipeline components select widget to the bottom
        w = schema.__delitem__('pipeline_components')
        schema.add(w)

        self.form = Form(schema, renderer=deform_renderer, **kwargs)
        return self.form

    def _apply_schema_edits(self, schemas, data):
        # assume the schemas have a contigous range of schema_position values
        # assume schemas are properly ordered

        def reordered(ss):
            for i, s in enumerate(ss):
                f = s['schema_position']
                f.default = f.missing = i
            return ss

        for i, s in enumerate(schemas):

            if "remove_%s_success" % s.name in data:
                del schemas[i]
                return reordered(schemas)

            if "move_up_%s_success" % s.name in data:
                if i == 0:
                    return schemas      # can't move a schema that's first
                # switch position between list members
                this, other = schemas[i], schemas[i-1]
                schemas[i-1] = this
                schemas[i] = other
                return reordered(schemas)

            if "move_down_%s_success" % s.name in data:
                if i == len(schemas) - 1:
                    return schemas      # can't move a schema that's last
                # switch position between list members
                this, other = schemas[i], schemas[i+1]
                schemas[i+1] = this
                schemas[i] = other
                return reordered(schemas)

        return schemas

    def show(self, form):
        # re-validate form, it is possible to be changed
        appstruct = {}
        controls = list(self.request.POST.items())
        if controls:
            print('have controls')

            try:
                appstruct = form.validate(controls)
            except deform.exception.ValidationFailure as e:
                print('failure, returning failure')
                return self.failure(e)

        schema = form.schema

        # now add new schemas, at the end of all others
        add_component = appstruct.get('pipeline_components')
        if add_component:
            p = pipeline_registry[add_component]
            s = p.schema(name=rand(10), title=p.title)
            f = s['schema_position']
            f.default = f.missing = len(schema.children)
            schema.add(s)
            appstruct['pipeline_components'] = ''

        # move pipeline_components to the bottom
        w = schema.__delitem__('pipeline_components')
        schema.add(w)

        form = Form(schema, buttons=self.buttons, renderer=deform_renderer,
                    **dict(self.form_options))

        # try to build a preview, if possible
        if appstruct.get('column'):
            pipeline = []

            for c in schema.children:
                _type = c.get('schema_type')

                if _type:
                    p = pipeline_registry[_type.default]
                    if c.name in appstruct:
                        kw = appstruct[c.name]
                    else:
                        kw = schema_defaults(c)

                    # remove auxiliary fields that are not expected as args
                    kw.pop('schema_position', None)
                    kw.pop('schema_type', None)

                    pipeline.append((p.name, kw))

            print(pipeline)
            content_stream = build_pipeline(
                self.document, appstruct['column'], pipeline
            )

            res = []
            for i in range(self.preview_size):
                try:
                    c = next(content_stream)
                except StopIteration:
                    break
                res.append(c)

            self.preview = res

        return {
            'form': form.render(appstruct),
        }


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
