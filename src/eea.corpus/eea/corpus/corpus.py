import json
import logging
import os.path

from eea.corpus.async import queue
from eea.corpus.processing import build_pipeline
from eea.corpus.utils import corpus_base_path, extract_corpus_id
from rq.decorators import job
from textacy import io

logger = logging.getLogger('eea.corpus')


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


CORPUS_CACHE = {}      # really dummy and simple way to cache corpuses


class Corpus(object):
    """ Corpus objects are, here, only a lightweight wrapper over a stream.

    It passes and caches items in the stream. Once the stream is consumed,
    the corpus restarts its stream and streams docs from cached memory
    """

    def __init__(self, file_name, corpus_id):

        cpath = corpus_base_path(file_name)
        fname = os.path.join(cpath, '%s_docs.json' % corpus_id)
        mname = os.path.join(cpath, '%s_info.json' % corpus_id)

        self._docs_stream = io.json.read_json(fname, lines=True)

        self._meta = next(io.json.read_json(mname))

        self._cache = []
        self._use_cache = False

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
