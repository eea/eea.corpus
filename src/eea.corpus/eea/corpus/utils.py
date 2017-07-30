from __future__ import unicode_literals
from collections import defaultdict
import hashlib
import json
import logging
import os
import textacy
# from textacy.fileio import split_record_fields


logger = logging.getLogger('eea.corpus')


CORPUS_STORAGE = "/corpus"


def load_corpus(file_name, corpus_id, **kw):
    """ Loads a textacy corpus from disk.

    Requires the document name and the corpus id
    """

    cpath = corpus_base_path(file_name)

    if os.listdir(cpath):
        assert os.path.exists(corpus_metadata_path(file_name, corpus_id))
        # if there are any files, assume the corpus is created
        # TODO: check that the corpus is really saved
        print("Saved corpus exists, loading", cpath, corpus_id)
        return textacy.Corpus.load(cpath, name=corpus_id)

    return None


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


def upload_location(file_name):
    """ Returns the path where an upload file would be saved, in the storage
    """

    assert not file_name.startswith('.')
    return os.path.join(CORPUS_STORAGE, file_name)


def is_valid_document(file_name):
    return file_name in os.listdir(CORPUS_STORAGE)


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
            if len(cfs) != 4:
                logger.warning("Not a valid corpus: %s (%s)",
                               file_name, corpus)
                continue
            res.append(corpus)

    return res


def get_corpus(request, doc=None, corpus_id=None):
    cache = request.corpus_cache
    if not (doc and corpus_id):
        doc, corpus_id = extract_corpus_id(request)

    if (doc not in cache) and (corpus_id not in cache.get(doc, [])):
        corpus = load_corpus(file_name=doc, corpus_id=corpus_id)

        if corpus is None:
            return None

        cache[doc] = {
            corpus_id: corpus
        }

    try:
        return cache[doc][corpus_id]
    except:
        import pdb; pdb.set_trace()


def corpus_metadata_path(file_name, corpus_id):
    """ Returns the zzz_eea.json file path for a given doc/corpus
    """
    cpath = corpus_base_path(file_name)      # corpus_id
    meta_name = "{0}_eea.json".format(corpus_id)
    meta_path = os.path.join(cpath, meta_name)
    return meta_path


def load_corpus_metadata(file_name, corpus_id):
    """ Returns the EEA specific metadata saved for a doc/corpus
    """
    meta_path = corpus_metadata_path(file_name, corpus_id)

    res = None

    with open(meta_path) as f:
        res = json.load(f)

    return res


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
                if len(cfs) != 4:
                    logger.warning("Not a valid corpus: %s (%s)", name, corpus)
                    continue
                meta = load_corpus_metadata(name, corpus)
                corpuses.append((corpus, meta))
        d = {
            'title': name,
            'name': name,
            'corpuses': corpuses
        }
        res.append(d)

    return res


def metadata(corpus):
    return {
        'docs': corpus.n_docs,
        'sentences': corpus.n_sents,
        'tokens': corpus.n_tokens,
        'lang': corpus.spacy_lang.lang,
    }


def extract_corpus_id(request):
    """ Extract document name (aka file_name) from request
    """

    md = request.matchdict or {}
    doc = md.get('doc')
    corpus = md.get('corpus')

    if not (is_valid_document(doc) and (corpus in available_corpus(doc))):
        return (None, None)

    return (doc, corpus)


def document_name(request):
    """ Extract document name (aka file_name) from request
    """

    md = request.matchdict or {}
    doc = md.get('doc')

    if not is_valid_document(doc):
        raise ValueError("Not a valid document: %s" % doc)

    return doc


def hashed_id(items):
    """ Generate a short id based on a list of items.

    The items should be in a stable, "hashable" form:
        - dictionaries should be converted to tuples (k, v) and sorted
    """
    # same options will generate the same corpus id
    m = hashlib.sha224()
    for kv in items:
        m.update(str(kv).encode('ascii'))
    return m.hexdigest()
