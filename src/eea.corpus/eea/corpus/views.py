from colander import Int, Schema, SchemaNode, String
from eea.corpus.topics import extract_topics
from eea.corpus.utils import load_or_create_corpus
from pyramid.httpexceptions import HTTPFound
from pyramid.view import view_config
from pyramid_deform import FormView
import colander
import deform
import os
import pandas as pd


CORPUS_PATH = "/corpus"


@view_config(route_name='home', renderer='templates/home.pt')
def home(request):
    existing = [f for f in os.listdir(CORPUS_PATH) if f.endswith('.csv')]
    return {'project': 'EEA Corpus Server', 'filelist': existing}


class Store(dict):
    def preview_url(self, name):
        return ""

tmpstore = Store()


class UploadSchema(Schema):
    # title = SchemaNode(String())
    upload = SchemaNode(
        deform.FileData(),
        widget=deform.widget.FileUploadWidget(tmpstore)
    )


def fpath(fname):
    assert not fname.startswith('.')
    fname = fname.split(os.path.sep)[-1]
    return os.path.join(CORPUS_PATH, fname)


class UploadView(FormView):
    schema = UploadSchema()
    buttons = ('save',)

    def save_success(self, appstruct):
        upload = appstruct.get('upload')
        if upload:
            fname = upload['filename']
            path = fpath(fname)
            with open(path, 'wb') as f:
                f.write(upload['fp'].read())

        self.request.session.flash(u"Your changes have been saved.")
        return HTTPFound(location='/')


@colander.deferred
def columns_widget(node, kw):

    request = kw['request']
    choices = []
    if 'f' in request.params:
        path = fpath(request.params['f'])
        f = pd.read_csv(path)
        choices = [(k, k) for k in f.keys()]

    return deform.widget.SelectWidget(values=choices)


class TopicsSchema(Schema):
    topics = SchemaNode(
        Int(),
        default=10,
        title="Number of topics to extract"
    )
    f = SchemaNode(
        String(),
        title="Filename",
        widget=deform.widget.HiddenWidget(),
        missing=colander.null,
    )
    column = SchemaNode(
        String(),
        widget=columns_widget,
        missing='',
    )


CACHE = {}


class TopicsView(FormView):
    schema = TopicsSchema()
    buttons = ('view',)

    vis = None
    metadata = None

    def document(self, text_column="text"):
        fname = self.request.params['f']
        if fname.startswith('.') or fname not in os.listdir(CORPUS_PATH):
            return None

        if fname not in CACHE:
            fpath = os.path.join(CORPUS_PATH, fname)
            corpus = load_or_create_corpus(fpath=fpath,
                                           text_column=text_column,
                                           normalize=True,
                                           optimize_phrases=True)
            CACHE[fname] = corpus

        return CACHE[fname]

    def view_success(self, appstruct):
        topics = appstruct['topics']
        column = appstruct['column']

        corpus = self.document(text_column=column)
        self.metadata = {
            'docs': corpus.n_docs,
            'sentences': corpus.n_sents,
            'tokens': corpus.n_tokens,
            'lang': corpus.spacy_lang.lang,
        }
        self.vis = extract_topics(corpus, topics)

    def appstruct(self):
        return {
            'f': self.request.params.get('f'),
            'topics': self.request.params.get('topics') or 10,
            'column': self.request.params.get('column') or '',
        }


def includeme(config):
    config.add_view(
        UploadView,
        name="upload",
        renderer="templates/simpleform.pt"
    )
    config.add_view(
        TopicsView,
        name="view",
        renderer="templates/topics.pt"
    )
