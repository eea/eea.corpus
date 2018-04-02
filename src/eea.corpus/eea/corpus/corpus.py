import json
import logging
import os.path
from collections import defaultdict

from eea.corpus.async import queue
from eea.corpus.config import CORPUS_STORAGE
from eea.corpus.processing import build_pipeline
from eea.corpus.utils import is_valid_document
from rq.decorators import job
from textacy import io

logger = logging.getLogger('eea.corpus')


def corpus_base_path(file_name):
    """ Returns the /corpus/var/<filename> folder for an uploaded file
    """

    varpath = os.path.join(CORPUS_STORAGE, 'var')
    base = os.path.join(varpath, file_name)

    if not os.path.exists(base):
        os.makedirs(base)

    return base


def delete_corpus(file_name, corpus_id):
    assert len(corpus_id) > 10
    cp = corpus_base_path(file_name)

    for f in os.listdir(cp):
        if f.startswith(corpus_id):
            fp = os.path.join(cp, f)
            os.unlink(fp)


def available_corpus(file_name):
    """ Returns available, already-created, corpuses for a filename

    The corpuses corespond to a column in the file.
    """

    base = corpus_base_path(file_name)

    if not os.path.exists(base):
        return []

    res = []
    files = defaultdict(list)

    for fn in os.listdir(base):
        if '_' not in fn:
            continue
        base, spec = fn.split('_', 1)
        files[base].append(spec)

        for corpus, cfs in files.items():
            if len(cfs) != len(('docs', 'info')):
                logger.warning("Not a valid corpus: %s (%s)",
                               file_name, corpus)

                continue
            res.append(corpus)

    return res


def corpus_info_path(file_name, corpus_id):
    """ Returns the <corpusid>_info.json file path for a given doc/corpus
    """
    cpath = corpus_base_path(file_name)      # corpus_id
    meta_name = "{0}_info.json".format(corpus_id)
    meta_path = os.path.join(cpath, meta_name)

    return meta_path


def load_corpus_metadata(file_name, corpus_id):
    """ Returns the EEA specific metadata saved for a doc/corpus
    """

    meta_path = corpus_info_path(file_name, corpus_id)

    res = None

    with open(meta_path) as f:
        res = json.load(f)

    return res


def extract_corpus_id(request):
    """ Extract document name (aka file_name) from request
    """

    md = request.matchdict or {}
    doc = md.get('doc')
    corpus = md.get('corpus')

    if not (is_valid_document(doc) and (corpus in available_corpus(doc))):
        return (None, None)

    return (doc, corpus)


def save_corpus_metadata(stats, file_name, corpus_id, text_column, **kw):
    cpath = corpus_base_path(file_name)      # corpus_id
    meta_name = "{0}_info.json".format(corpus_id)
    meta_path = os.path.join(cpath, meta_name)

    title = kw.pop('title')
    description = kw.pop('description', '')

    info = {
        'title': title,
        'description': description,
        'statistics': stats,
        'text_column': text_column,
        'kw': kw,
    }
    with open(meta_path, 'w') as f:
        json.dump(info, f)


class DocStream:
    """ A pass-through stream that gathers stats on streamed docs
    """

    def __init__(self, docs):
        self.docs = docs
        # self.n_tokens = 0
        # self.n_sents = 0
        self.n_docs = 0

    def __iter__(self):
        for doc in self.docs:
            # self.n_tokens += doc.n_tokens
            # self.n_sents += doc.n_sents
            self.n_docs += 1
            yield doc

    def get_statistics(self):
        return {
            'docs': self.n_docs,
            # 'sentences': self.n_sents,
            # 'tokens': self.n_tokens,
            'lang': 'en',
        }


@job(queue=queue)
def build_corpus(pipeline, corpus_id, file_name, text_column, **kw):
    """ Async job to build a corpus using the provided pipeline
    """

    cpath = corpus_base_path(file_name)      # corpus_id
    fname = os.path.join(cpath, '%s_docs.json' % corpus_id)
    logger.info('Creating corpus for %s at %s', file_name, cpath)

    docs = build_pipeline(file_name, text_column, pipeline, preview_mode=False)

    stream = DocStream(docs)

    io.json.write_json(
        stream, fname, mode='wt', lines=True, ensure_ascii=True,
        separators=(',', ':')
    )
    save_corpus_metadata(
        stream.get_statistics(), file_name, corpus_id, text_column, **kw
    )


class Corpus(object):
    """ Corpus objects are just a lightweight wrapper over a stream.

    They pass and cache items in the stream. Once the stream is consumed, the
    corpus restarts its stream and streams docs from cached memory
    """

    def __init__(self, file_name, corpus_id):
        self.file_name = file_name
        self.corpus_id = corpus_id

        self._cache = []
        self._use_cache = False

        cpath = corpus_base_path(file_name)
        fname = os.path.join(cpath, '%s_docs.json' % corpus_id)

        self._docs_stream = io.json.read_json(fname, lines=True)
        self._meta = load_corpus_metadata(file_name, corpus_id)

    def __iter__(self):
        if self._use_cache:
            return iter(self._cache)

        return self

    def __next__(self):
        try:
            value = next(self._docs_stream)
        except StopIteration:
            self._use_cache = True
            raise
        else:
            self._cache.append(value)

        return value

    @property
    def n_docs(self):
        # [{'kw': {'column': 'Text', 'pipeline_components': ''}, 'text_column':
        #   'Text', 'statistics': {'lang': 'en', 'docs': 3002}, 'description':
        #   '', 'title': 'noop'}]

        return self._meta['statistics']['docs']

    @property
    def title(self):
        return self._meta['title']

    @property
    def description(self):
        return self._meta['description']


def get_corpus(request, doc=None, corpus_id=None):
    if not (doc and corpus_id):
        doc, corpus_id = extract_corpus_id(request)

    corpus = Corpus(file_name=doc, corpus_id=corpus_id)

    return corpus


def available_documents(request):
    """ Returns a list of available documents (ex: csv files) in the storage
    """

    res = []

    docs = [f for f in os.listdir(CORPUS_STORAGE) if f.endswith('.csv')]

    for name in docs:
        cpath = corpus_base_path(name)
        corpuses = []

        if os.path.exists(cpath):
            files = defaultdict(list)

            for fn in os.listdir(cpath):
                if '_' not in fn:
                    continue
                base, spec = fn.split('_', 1)
                files[base].append(spec)

            for corpus, cfs in files.items():
                if len(cfs) != len(('docs', 'info')):
                    logger.warning("Not a valid corpus: %s (%s)", name, corpus)

                    continue
                corpus = get_corpus(request, doc=name, corpus_id=corpus)
                corpuses.append(corpus)
                # meta = load_corpus_metadata(name, corpus)
                # corpuses.append((corpus, meta))
        d = {
            'title': name,
            'name': name,
            'corpuses': corpuses
        }
        res.append(d)

    return res
