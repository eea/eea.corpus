from eea.corpus.async import queue
from rq.decorators import job


@job(queue=queue)
def build_phrases(pipeline, file_name, text_column, **kw):
    """ Async job to build a phrase models using the provided pipeline
    """

    content = build_pipeline(file_name, text_column, pipeline,
                             preview_mode=False)
    pid = component_phash_id(file_name, text_column, pipeline)

    logger.info("Phrase processor: producing phrase model %s", cache_path)
    build_phrase_models(content, cache_path, settings['level'])
