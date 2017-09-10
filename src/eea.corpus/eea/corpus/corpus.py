from eea.corpus.async import queue
from eea.corpus.processing import build_pipeline
from eea.corpus.utils import corpus_base_path
from eea.corpus.utils import extract_corpus_id
from rq.decorators import job
from textacy import fileio, Corpus
# from textacy.doc import Doc
import json
import logging
import os.path


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
        self.n_tokens = 0
        self.n_sents = 0
        self.n_docs = 0

    def __iter__(self):
        for doc in self.docs:
            self.n_tokens += doc.n_tokens
            self.n_sents += doc.n_sents
            self.n_docs += 1
            yield doc

    def get_statistics(self):
        return {
            'docs': self.n_docs,
            'sentences': self.n_sents,
            'tokens': self.n_tokens,
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

    sdocs = DocStream(docs)
    docs = ({'text': doc.text,
             'metadata': doc.metadata} for doc in sdocs)

    fileio.write_json_lines(
        docs, fname, mode='wt',
        ensure_ascii=True, separators=(',', ':')
    )
    save_corpus_metadata(
        sdocs.get_statistics(), file_name, corpus_id, text_column, **kw
    )


def load_corpus(file_name, corpus_id):
    """ Loads a textacy corpus from disk.

    Requires the document name and the corpus id
    """

    cpath = corpus_base_path(file_name)
    fname = os.path.join(cpath, '%s_docs.json' % corpus_id)

    # TODO: we shouldn't hardcode the language
    corpus = Corpus('en')

    texts = []
    metas = []

    with open(fname, 'rt') as f:
        for line in f:
            try:
                j = json.loads(line)
            except:
                logger.warning("Could not load corpus line: %r", line)
                continue
            texts.append(j['text'])
            metas.append(j['metadata'])

    corpus.add_texts(texts, metadatas=metas)
    return corpus


def get_corpus(request, doc=None, corpus_id=None):
    if not (doc and corpus_id):
        doc, corpus_id = extract_corpus_id(request)

    assert doc and corpus_id
    # corpus = load_corpus(file_name=doc, corpus_id=corpus_id)

    cache = request.corpus_cache
    if corpus_id not in cache.get(doc, []):
        corpus = load_corpus(file_name=doc, corpus_id=corpus_id)

        if corpus is None:
            return None

        cache[doc] = {
            corpus_id: corpus
        }

    return cache[doc][corpus_id]
