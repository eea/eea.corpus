from __future__ import unicode_literals
from collections import defaultdict
from cytoolz import compose
from textacy.doc import Doc
import hashlib
import json
import logging
import os
import random
import string


logger = logging.getLogger('eea.corpus')


CORPUS_STORAGE = "/corpus"


def rand(n):
    return ''.join(random.sample(string.ascii_uppercase + string.digits, k=n))


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
                meta = load_corpus_metadata(name, corpus)
                corpuses.append((corpus, meta))
        d = {
            'title': name,
            'name': name,
            'corpuses': corpuses
        }
        res.append(d)

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

    >>> raise ValueError

    The items should be in a stable, "hashable" form:
        - dictionaries should be converted to tuples (k, v) and sorted

    """
    # same options will generate the same corpus id
    m = hashlib.sha224()
    for kv in items:
        m.update(str(kv).encode('ascii'))
    return m.hexdigest()


def set_text(doc, text):
    """ Build a new doc based on doc's metadata and provided text
    """

    return Doc(text, metadata=doc.metadata, lang='en')


def is_locked(fpath):
    """ Check if a lock file exists for given path
    """
    path = fpath + '.lock'
    return os.path.exists(path)


def schema_defaults(schema):
    """ Returns a mapping of fielname:defaultvalue
    """
    res = {}
    for child in schema.children:
        if child.default is not None:
            res[child.name] = child.default
        else:
            res[child.name] = child.missing
    return res


def tokenize(phrase, delimiter='_'):
    """ Tokenizes a phrase (converts those words to a unique token)
    """

    words = phrase.split(' ')
    res = []

    # remove the 's in text
    for w in words:
        w = w.split("'")[0]
        res.append(w)

    return delimiter.join(res)


# from spacy.tokens.doc import Doc as SpacyDoc
# def is_safe_to_save(doc):
#     """ Is this doc safe to save?
#
#     For some reason there's a bug in saving/loading spacy Docs. Here we test
#     that the doc can be loaded back from its serialized representation.
#
#     For further reference, see:
#
#         * https://github.com/explosion/spaCy/issues/1045
#         * https://github.com/explosion/spaCy/issues/985
#
#     """
#     text = doc.text[:100]
#     vocab = doc.spacy_vocab
#     bs = doc.spacy_doc.to_bytes()
#     try:
#         SpacyDoc(vocab).from_bytes(bs)
#         return True
#     except Exception:
#         logger.warning("Will not save %s, it will not be loadable", text)
#         return False


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
    """ Tokenizes text. Returns lists of tokens (words)
    """
    ignore_chars = "()*:\"><][#\n\t'^%?=&"
    for c in ignore_chars:
        text = text.replace(c, ' ')
    words = text.split(' ')

    text = list(handle_text(words))

    return text
