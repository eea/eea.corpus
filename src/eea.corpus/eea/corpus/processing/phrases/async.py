from eea.corpus.async import queue
from eea.corpus.processing import build_pipeline
from eea.corpus.processing.phrases.phrases import build_phrase_models
from eea.corpus.utils import corpus_base_path
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

    pipeline = pipeline[:-1]  # last is phrases

    content = build_pipeline(
        file_name, text_column, pipeline, preview_mode=False
    )

    logger.info("Phrase processor: producing phrase model %s", cache_path)
    build_phrase_models(content, cache_path, settings)
