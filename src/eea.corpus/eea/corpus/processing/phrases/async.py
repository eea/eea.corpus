from eea.corpus.async import queue
from eea.corpus.processing import build_pipeline
from eea.corpus.utils import corpus_base_path
from gensim.models.phrases import Phrases
from itertools import tee
from rq.decorators import job
import logging
import os.path

logger = logging.getLogger('eea.corpus')


@job(
    queue=queue,
    timeout='1h',
    ttl='15m',  # abort job if not picked up in 15m
)
def build_phrases(pipeline, file_name, text_column, phash_id, settings):
    """ Async job to build a phrase models using the provided pipeline
    """

    base_path = corpus_base_path(file_name)
    cache_path = os.path.join(base_path, '%s.phras' % phash_id)

    content = build_pipeline(
        file_name, text_column, pipeline, preview_mode=False
    )

    logger.info("Phrase processor: producing phrase model %s", cache_path)
    build_phrase_models(content, cache_path, settings)


def build_phrase_models(content, base_path, settings):
    """ Build and save the phrase models
    """

    ngram_level = settings['level']

    # According to tee() docs, this may be inefficient in terms of memory.
    # We need to do this because we need multiple passes through the
    # content stream.
    cs1, cs2 = tee(content, 2)

    for i in range(ngram_level-1):
        phrases = Phrases(cs1)
        path = "%s.%s" % (base_path, i + 2)     # save path as n-gram level
        logger.info("Phrase processor: Saving %s", path)
        phrases.save(path)
        content = phrases[cs2]  # tokenize phrases in content stream
        cs1, cs2 = tee(content, 2)

    # return iter(content)    # is a gensim TransformedCorpus
