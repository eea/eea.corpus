from eea.corpus.async import queue
from eea.corpus.processing import build_pipeline
from eea.corpus.utils import corpus_base_path
from rq.decorators import job
from textacy import fileio
import json
import logging
import os.path


logger = logging.getLogger('eea.corpus')


def save_corpus_metadata(stats, file_name, corpus_id, text_column, **kw):
    cpath = corpus_base_path(file_name)      # corpus_id
    meta_name = "{0}_eea.json".format(corpus_id)
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

    docs = DocStream(docs)
    docs = ({'text': doc.text, 'metadata': doc.metadata} for doc in docs)

    fileio.write_json_lines(
        docs, fname, mode='wb',
        ensure_ascii=False, separators=(',', ':')
    )
    save_corpus_metadata(
        docs.get_statistics(), file_name, corpus_id, text_column, **kw
    )
