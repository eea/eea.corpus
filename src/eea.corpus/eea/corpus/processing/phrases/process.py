""" Build a list of collocations (phrases) and show them in the stream.

Normally it would be impossible to use the phrase detection in "real-time", as
the phrases need to be detected in the entire corpus. To overcome this, we do:

    * Try to load a phrase model suitable for the current pipeline.
    * If one doesn't exist, schedule an async job that will generate the phrase
      model
    * Inform the user, in the UI, about the availability of the preview
"""

from eea.corpus.async import get_assigned_job
from eea.corpus.processing import pipeline_component
from eea.corpus.processing.phrases.async import build_phrases
from eea.corpus.processing.phrases.phrases import use_phrase_models
from eea.corpus.processing.phrases.schema import PhraseFinder
from eea.corpus.processing.phrases.utils import get_job_finish_status
from eea.corpus.processing.phrases.utils import phrase_model_files
from eea.corpus.processing.utils import get_pipeline_for_component
from eea.corpus.utils import corpus_base_path
from eea.corpus.utils import to_doc
from redis.exceptions import ConnectionError
import logging

logger = logging.getLogger('eea.corpus')


@pipeline_component(schema=PhraseFinder,
                    title="Find and process phrases")
def process(content, env, **settings):       # pipeline, preview_mode,
    """ Phrases detection and processing
    """

    content = (to_doc(doc) for doc in content)

    # TODO: this is just to avoid errors on not installed language models
    # In the future, this should be treated properly or as a pipeline component
    content = (doc for doc in content if doc.lang == 'en')

    yield from cached_phrases(content, env, settings)

    if env['preview_mode']:
        yield from preview_phrases(content, env, settings)
    else:
        yield from produce_phrases(content, env, settings)


def preview_phrases(content, env, settings):
    """ Tokenized phrases in preview mode

    When running in preview mode, check for an already running job with this
    phash_id. If no such job, schedule a new job.
    """

    file_name = env['file_name']
    text_column = env['text_column']
    phash_id = env['phash_id']

    # If saved phrase models exist, we don't do anything, because the
    # ``cached_phrases`` should have taken care of it
    base_path = corpus_base_path(file_name)
    files = phrase_model_files(base_path, phash_id)
    if files:
        raise StopIteration

    logger.info("Phrase processing: need phrase model id %s", phash_id)

    job = get_assigned_job(phash_id)
    if job is None:
        phrase_model_pipeline = get_pipeline_for_component(env)
        try:
            job = build_phrases.delay(
                phrase_model_pipeline,
                file_name,
                text_column,
                phash_id,
                settings,
            )
            job.meta = {'phash_id': phash_id}
            job.save_meta()
            logger.warning("Phrase processing: enqueued a new job %s", job.id)
        except ConnectionError:       # swallow the error
            logger.warning("Phrase processing: could not enqueue a job")
    else:
        logger.info("Preview phrases: found a job (%s), passing through",
                    job.id)

    yield from content


def produce_phrases(content, env, settings):
    """ Produce a transformed stream of text.

    If cache files already exist, we do nothing, assuming that the processing
    function already used them.

    Otherwise, look for an async job already doing phrase model building. If
    one exists, wait for its status to change, to benefit from its cache files.
    If no such job exists, produce those files here.

    ``produce_phrases`` is usually called from an async job runner, so we can
    assume that any other jobs would already have been picked up or failed,
    but at least there is a queue that is already processing.
    """

    # If saved phrase models exist, we don't do anything, because the
    # ``cached_phrases`` should have taken care of it

    file_name = env['file_name']
    text_column = env['text_column']
    phash_id = env['phash_id']

    base_path = corpus_base_path(file_name)
    files = phrase_model_files(base_path, phash_id)
    if files:
        raise StopIteration

    if not get_job_finish_status(phash_id):
        # something wrong with the job, forcing build phrases
        phrase_model_pipeline = get_pipeline_for_component(env)
        logger.info("Phrase processor: producing phrase model %s", phash_id)
        job = build_phrases(
            phrase_model_pipeline,
            file_name,
            text_column,
            phash_id,
            settings,
        )
        job.meta = {'phash_id': phash_id},
        job.save_meta()


    yield from cached_phrases(content, env, settings)


def cached_phrases(content, env, settings):
    """ Returns tokenized phrases using saved phrase models.

    The phrase models are saved using a common name, like:

        <phash_id>.phras.X

    where X is the n-gram level (one of 2,3,4).

    The phrase models are loaded and process the content stream.

    We can run in several modes:

        * remove all text but leave the phrases tokens
        * append the collocations to the document
        * replace the collocations where they appear

        TODO: implement the above

    If there aren't any cached phrase models, we don't yield anything.

    # TODO: should not do from_iterable. This collapses the docs to sentences
    # and distorts the result stream
    """
    base_path = corpus_base_path(env['file_name'])
    files = phrase_model_files(base_path, env['phash_id'])
    if not files:
        raise StopIteration

    logger.info("Phrase processor: using phrase models from %s", base_path)

    yield from use_phrase_models(content, files, settings)
