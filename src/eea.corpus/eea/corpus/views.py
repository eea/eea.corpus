""" Pyramid views. Main UI for the eea.corpus
"""

from deform import Button
from deform import Form
from deform import ZPTRendererFactory
from eea.corpus.async import queue
from eea.corpus.corpus import build_corpus
from eea.corpus.corpus import get_corpus
from eea.corpus.processing import build_pipeline
from eea.corpus.processing import pipeline_registry
from eea.corpus.schema import ClassifficationModelSchema
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
from eea.corpus.utils import hashed_id
from eea.corpus.utils import rand
from eea.corpus.utils import schema_defaults
from eea.corpus.utils import upload_location
from cytoolz import compose
from itertools import islice
from peppercorn import parse
from pkg_resources import resource_filename
from pyramid.httpexceptions import HTTPFound
from pyramid.renderers import render
from pyramid.view import view_config
from pyramid_deform import FormView
import deform
import logging
import pyramid.httpexceptions as exc
import sys
import traceback as tb

logger = logging.getLogger('eea.corpus')

# Configure alternative Deform templates renderer. Includes overrides for
# default deform templates
deform_templates = resource_filename('deform', 'templates')
eeacorpus_templates = resource_filename('eea.corpus', 'templates/deform')
search_path = (eeacorpus_templates, deform_templates)
deform_renderer = ZPTRendererFactory(search_path)


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
                for line in upload['fp']:
                    f.write(line)

        self.request.session.flash(u"Your changes have been saved.")
        return HTTPFound(location='/')


@view_config(
    route_name="corpus_topics",
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
        corpus = self.corpus()
        return {
            'docs': corpus.n_docs,
            'sentences': corpus.n_sents,
            'tokens': corpus.n_tokens,
            'lang': corpus.lang,
        }

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

    def _get_sorted_component_names(self, request):
        """ Returns a list of (component names, params) from request
        """

        data = parse(request.POST.items())
        schemas = []        # establish position of widgets
        for k, v in self.request.POST.items():
            if (k == '__start__') and (':mapping' in v):
                sk = v.split(':')[0]
                parms = data[sk]
                if isinstance(parms, dict) and parms.get('schema_type', None):
                    schemas.append((sk, data[sk]))

        return schemas

    def get_pipeline_components(self):
        """ Returns a pipeline, a list of (process, schema name, arguments)

        It uses the request to understand the structure of the pipeline. The
        significant elements of that structure are the pipeline component name,
        its position in the schema and its settings.

        It's only used in ``generate_corpus_success`` in this form.
        """

        pipeline = []

        for name, params in self._get_sorted_component_names(self.request):
            kwargs = params.copy()
            kwargs.pop('schema_type')
            s = (params['schema_type'], name, kwargs)
            pipeline.append(s)

        return pipeline

    def pipeline_from_schema(self, schema, appstruct):

        # the difference to _get_sorted_component_names is that here
        # we loop over schema children and need to have default params when
        # schema is newly added
        pipeline = []

        for c in schema.children:
            _type = c.get('schema_type')
            if _type:
                # assume mapping schema
                kw = appstruct.get(c.name, schema_defaults(c)).copy()

                # remove auxiliary fields that are not expected as args
                kw.pop('schema_type', None)

                # TODO: is the pipeline_registry here needed?
                p = pipeline_registry[_type.default]
                pipeline.append((p.name, c.name, kw))

        return pipeline

    def _schemas(self):
        """ Returns a list of schema instances, for ``Form`` instantiation.
        """

        schemas = []
        for name, params in self._get_sorted_component_names(self.request):
            _type = params['schema_type']
            p = pipeline_registry[_type]
            s = p.schema(name=name, title=p.title)
            schemas.append(s)

        return schemas

    def preview_success(self, appstruct):
        # preview is done by show()
        pass

    def generate_corpus_success(self, appstruct):
        pipeline = self.get_pipeline_components()

        s = appstruct.copy()
        s['doc'] = self.document
        corpus_id = hashed_id(sorted(s.items()))

        job = queue.enqueue(build_corpus,
                            timeout='1h',
                            args=(
                                pipeline,
                                corpus_id,
                                self.document,
                                appstruct['column'],
                            ),
                            kwargs=appstruct)

        raise exc.HTTPFound('/job-view/%s/%s/job/%s' %
                            (self.document, corpus_id, job.id))

    def form_class(self, schema, **kwargs):
        data = parse(self.request.POST.items())

        schemas = self._schemas()
        schemas = self._apply_schema_edits(schemas, data)
        for s in schemas:
            schema.add(s)

        # move the pipeline components select widget to the bottom
        w = schema.__delitem__('pipeline_components')
        schema.add(w)

        kwargs.update(dict(self.form_options))

        self.form = Form(schema, renderer=deform_renderer, **kwargs)
        return self.form

    def _apply_schema_edits(self, schemas, data):
        # assume the schemas have a contigous range of schema_position values
        # assume schemas are properly ordered

        for i, s in enumerate(schemas):

            if "remove_%s_success" % s.name in data:
                del schemas[i]
                return schemas

            if "move_up_%s_success" % s.name in data:
                if i == 0:
                    return schemas      # can't move a schema that's first
                # switch position between list members
                this, other = schemas[i], schemas[i-1]
                schemas[i-1] = this
                schemas[i] = other
                return schemas

            if "move_down_%s_success" % s.name in data:
                if i == len(schemas) - 1:
                    return schemas      # can't move a schema that's last
                # switch position between list members
                this, other = schemas[i], schemas[i+1]
                schemas[i+1] = this
                schemas[i] = other
                return schemas

        return schemas

    def show(self, form):
        # re-validate form, it is possible to be changed
        appstruct = {}
        controls = list(self.request.POST.items())
        if controls:
            try:
                appstruct = form.validate(controls)
            except deform.exception.ValidationFailure as e:
                return self.failure(e)

        schema = form.schema
        # now add new schemas, at the end of all others
        add_component = appstruct.pop('pipeline_components', None)
        if add_component:
            p = pipeline_registry[add_component]
            s = p.schema(name=rand(10), title=p.title,)
            schema.add(s)
            # move pipeline_components to the bottom
            w = schema.__delitem__('pipeline_components')
            schema.add(w)

        # try to build a preview, if possible
        if appstruct.get('column'):
            pipeline = self.pipeline_from_schema(schema, appstruct)
            pstruct = self.request.create_corpus_pipeline_struct = {
                'file_name': self.document,
                'text_column': appstruct['column'],
                'pipeline': pipeline,
                'preview_mode': True
            }
            content_stream = build_pipeline(**pstruct)

            self.preview = islice(content_stream, 0, self.preview_size)

        form = Form(schema, buttons=self.buttons, renderer=deform_renderer,
                    **dict(self.form_options))
        reqts = form.get_widget_resources()

        return {
            'form': form.render(appstruct),
            'css_links': reqts['css'],
            'js_links': reqts['js'],
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


@view_config(route_name="corpus_view", renderer='templates/view_corpus.pt')
def view_corpus(request):
    page = int(request.matchdict['page'])
    corpus = get_corpus(request)

    if corpus is None or page > (corpus.n_docs - 1):
        raise exc.HTTPNotFound()

    nextp = page + 1
    if nextp >= corpus.n_docs:
        nextp = None

    prevp = page - 1
    if prevp < 0:
        prevp = None

    return {
        'corpus': corpus,
        'doc': corpus[page],
        'nextp': nextp,
        'prevp': prevp,
        'page': page
    }


def handle_slash(words):
    for word in words:
        for bit in word.split('/'):
            yield bit


def handle_numbers(words):
    for word in words:
        if word.isnumeric():
            yield "*number*"
        yield word


def lower_words(words):
    yield from (w.lower() for w in words)


def filter_small_words(words):
    for w in words:
        if len(w) > 2:
            yield w


handle_text = compose(filter_small_words, lower_words, handle_numbers,
                      handle_slash, )


def tokenizer(text):
    ignore_chars = "()*:\"><][#\n\t'^%?=&"
    for c in ignore_chars:
        text = text.replace(c, ' ')
    words = text.split(' ')

    text = list(handle_text(words))

    return text


class ClassVocab:
    def __init__(self):
        self.vocab = {}

    def __getitem__(self, k):
        if isinstance(k, float):
            k = 'empty'
        k = [x for x in k.split('/') if x][0]
        if k not in self.vocab:
            x = len(self.vocab)
            self.vocab[k] = x
            return x
        return self.vocab[k]


@view_config(route_name="corpus_classify", renderer='templates/classify.pt')
class CreateClassificationModelView(FormView):
    schema = ClassifficationModelSchema()
    buttons = ('classify', 'fasttext')

    score = None

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
        corpus = self.corpus()
        return {
            'docs': corpus.n_docs,
            'sentences': corpus.n_sents,
            'tokens': corpus.n_tokens,
            'lang': corpus.lang,
        }

    def classify_success(self, appstruct):
        # conventions: X are features, y are labels
        # X_train is array of training feature values,
        # X_test is array with test values
        # y_train are labels for X_train, y_test are labels for X_test

        from sklearn import metrics
        from sklearn.model_selection import train_test_split
        from itertools import tee

        corpus = self.corpus()
        docs = (doc for doc in corpus
                if not isinstance(doc.metadata['Category Path'], float))
        docs_stream, meta_stream = tee(docs, 2)

        print("Transforming docs")
        docs = [doc.text for doc in docs_stream]

        from sklearn.feature_extraction.text import CountVectorizer
        vect = CountVectorizer(input='content', strip_accents='unicode',
                               tokenizer=tokenizer,  # stop_words='english',
                               max_features=5000)

        X = vect.fit_transform(docs)

        from sklearn.feature_extraction.text import TfidfTransformer
        transf = TfidfTransformer()
        X = transf.fit_transform(X)
        # X = X.toarray()   # only needed for GDC

        # from sklearn.feature_extraction.text import TfidfVectorizer
        # vect = TfidfVectorizer(max_features=5000,
        #                        ngram_range=(1, 3), sublinear_tf=True)
        # X = vect.fit_transform(docs)

        # from sklearn.ensemble import RandomForestClassifier
        # model = RandomForestClassifier(n_estimators=100)    # acc: 0.73

        # from sklearn import svm
        # model = svm.SVC(kernel='poly', degree=3, C=1.0)     # acc: 0.66

        # from sklearn.naive_bayes import MultinomialNB       # acc: 0.73
        # model = MultinomialNB(alpha=0.1)        # , fit_prior=True

        # takes a long time, can go higher if more estimators, higher l_rate
        # from sklearn.ensemble import GradientBoostingClassifier   # acc: 0.65
        # model = GradientBoostingClassifier(n_estimators=10,learning_rate=0.1)

        # 0.763 with tfidf from countvect 5000, 0.7 without tfidf
        from sklearn.linear_model import LogisticRegression
        model = LogisticRegression()

        vocab = ClassVocab()
        y = [vocab[doc.metadata['Category Path']] for doc in meta_stream]

        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.1, random_state=3311)

        print("Training on %s docs" % str(X_train.shape))

        model.fit(X_train, y_train)

        print("Fitting model")
        model.fit(X_train, y_train)
        print("done")

        pred = model.predict(X_test)
        self.score = metrics.accuracy_score(y_test, pred)
        print(self.score)

    def fasttext_success(self, appstruct):
        from itertools import islice
        # from pyfasttext import FastText

        corpus = self.corpus()
        docs = [doc for doc in corpus
                if not isinstance(doc.metadata['Category Path'], float)]

        split = int(corpus.n_docs * 0.9)        # TODO: should be docs

        train_docs = islice(docs, 0, split)
        test_docs = islice(docs, split, corpus.n_docs)

        print('Writing corpus to disk')
        lines = []
        for doc in train_docs:
            labels = doc.metadata['Category Path'].replace('/', ' __label__')
            labels = labels.strip()
            # labels = '__label__' + doc.metadata['Category Path'].split('/')[1]
            text = doc.text.replace('\n', ' ')
            line = " ".join([labels, text])
            lines.append(line)

        import unicodedata
        with open('/tmp/corpus-train.txt', 'wb') as f:
            s = '\n'.join(lines)
            s = unicodedata.normalize('NFKD', s).encode('ascii', 'ignore')
            f.write(s)

        y_test = []
        test_lines = []
        with open('/tmp/corpus-test.txt', 'w') as f:
            for doc in test_docs:
                labels = [x for x in doc.metadata['Category Path'].split('/')
                          if x]
                # labels = '__label__' + \
                #     doc.metadata['Category Path'].split('/')[1]
                test_lines.append(doc.text.replace('\n', ' '))
                y_test.append(labels)
            f.write('\n'.join(test_lines))

        print("Training model")
        # model = fasttext.supervised()
        import fasttext as ft
        model = ft.supervised(input_file='/tmp/corpus-train.txt',
                              output='/tmp/ftmodel', epoch=100)
        print("Model trained")

        # from sklearn import metrics
        # self.score = metrics.accuracy_score(y_test, pred)

        pred = model.predict(test_lines, k=2)
        zz = list(zip(pred, y_test))
        tt = [x for x in zz if set(x[0]) != set(x[1])]
        notok = len(tt)
        self.score = notok * 100 / len(zz)
        print("Score %s" % self.score)

        xx = model.predict_proba(test_lines, k=2)
        import pdb; pdb.set_trace()
