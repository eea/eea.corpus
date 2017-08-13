""" Build a list of collocations (phrases) and show them in the stream.

Normally it would be impossible to use the phrase detection in "real-time", as
the phrases need to be detected in the entire corpus. To overcome this, we do:

    * Try to load a phrase model suitable for the current pipeline.
    * If one doesn't exist, schedule an async job that will generate the phrase
      model
    * Inform the user, in the UI, about the availability of the preview
"""

# from eea.corpus.processing import build_pipeline
from eea.corpus.async import queue
from eea.corpus.async import get_assigned_job
from eea.corpus.processing import get_pipeline_for_component
from eea.corpus.processing import pipeline_component
from eea.corpus.processing.phrases.schema import PhraseFinder
from eea.corpus.processing.utils import component_phash_id
from eea.corpus.utils import corpus_base_path
from gensim.models.phrases import Phrases
from itertools import chain
from itertools import tee
from redis.exceptions import ConnectionError
from textacy.doc import Doc
import logging
import os.path

logger = logging.getLogger('eea.corpus')


@pipeline_component(schema=PhraseFinder,
                    title="Find and process phrases")
def process(content, env, **settings):       # pipeline, preview_mode,
    """ We can run in several modes:

    * remove all text but leave the phrases tokens
    * append the collocations to the document
    * replace the collocations where they appear

    TODO: implement the above
    """

    # convert content stream to ``textacy.doc.Doc``
    # TODO: treat case when doc is a list of words.
    content = (isinstance(doc, str) and Doc(doc) or doc for doc in content)
    # TODO: this is just to avoid errors on not installed language models
    # In the future, this should be treated properly or as a pipeline component
    content = (doc for doc in content if doc.lang == 'en')

    # tokenized text is list of statements, chain them to make list of tokens
    content = chain.from_iterable(doc.tokenized_text for doc in content)

    file_name = env['file_name']
    text_column = env['text_column']
    phrase_model_pipeline = get_pipeline_for_component(env)

    phash_id = component_phash_id(
        file_name, text_column, phrase_model_pipeline
    )
    logger.info("Phrase processing: need phrase model id %s", phash_id)

    base_path = corpus_base_path(file_name)
    cache_path = os.path.join(base_path, '%s.phras' % phash_id)

    # TODO: test that the phrase model file is "finished"
    # maybe use transactional file system?
    for f in os.listdir(base_path):
        if f.startswith(cache_path):    # it's an ngram model
            # TODO: enable this right now
            # yield from cached_phrases(cache_path, content)
            raise StopIteration

    if not env.get('preview_mode'):  # production mode, generate phrase model
        logger.info("Phrase processor: producing phrase model %s", cache_path)
        content = build_phrase_models(content, cache_path, settings['level'])
        # TODO: enable this right now
        # yield from content
        raise StopIteration

    # if running in preview mode, look for an async job already doing
    # phrase model building
    job = get_assigned_job(phash_id)
    job = None      # TODO: end debug

    if job is None:
        try:
            job = queue.enqueue(build_phrases,
                                timeout='1h',
                                args=(
                                    phrase_model_pipeline,
                                    file_name,
                                    text_column,
                                ),
                                meta={'phash_id': phash_id},
                                kwargs={})
            logger.warning("Phrase processing: enqueued a new job %s", job.id)
        except ConnectionError:
            # swallow the error
            logger.warning("Phrase processing: could not enqueue a job")
    else:
        logger.info("Phrase processor: found a job (%s), passing through",
                    job.id)

    # TODO: enable this right now
    # yield from content  # TODO: this is now tokenized text, should fix


logger = logging.getLogger('eea.corpus')


def build_phrase_models(content, cache_path, settings):
    """ Build and save the phrase models
    """

    ngram_level = settings['level']

    # According to tee() docs, this may be inefficient in terms of memory.
    # We need to do this because we need multiple passes through the
    # content stream.
    cs1, cs2 = tee(content, 2)

    for i in range(ngram_level-1):
        phrases = Phrases(cs1)
        path = "%s.%s" % (cache_path, i + 1)
        logger.info("Phrase processor: Saving %s", path)
        phrases.save(path)
        content = phrases[cs2]  # tokenize phrases in content stream
        cs1, cs2 = tee(content, 2)

    return iter(content)    # is a gensim TransformedCorpus


def cached_phrases(cache_path, content):
    """ Returns tokenized phrases using saved phrase models.

    The phrase models are saved using a commong name, like:

        <phash_id>.phras.X

    where X is the n-gram level (one of 2,3,4).

    The phrase models are loaded and process the content stream.
    """

    content = chain.from_iterable(doc.tokenized_text for doc in content)

    logger.info("Phrase processor: using phrase models from %s", cache_path)

    phrase_model_files = []
    base_path, base_name = os.path.split(cache_path)
    for name in sorted(os.listdir(base_path)):
        if name.startswith(base_name):
            phrase_model_files.append(os.path.join(base_path, name))

    for fpath in phrase_model_files:
        phrases = Phrases.load(fpath)
        content = phrases[content]

    # convert list of words back to full text document
    content = (
        ". ".join(
            (" ".join(w for w in sent) for sent in content)
        )
    )
    # TODO: enable this right now
    # yield from content
