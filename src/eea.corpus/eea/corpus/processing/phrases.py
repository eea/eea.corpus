""" Build a list of collocations (phrases) and show them in the stream.

Normally it would be impossible to use the phrase detection in "real-time", as
the phrases need to be detected in the entire corpus. To overcome this, we do:

    * Try to load a phrase model suitable for the current pipeline.
    * If one doesn't exist, schedule an async job that will generate the phrase
      model
    * Inform the user, in the UI, about the availability of the preview
"""

from colander import Schema
from deform.widget import MappingWidget
from eea.corpus.async import queue
from eea.corpus.processing import build_pipeline
from eea.corpus.processing import pipeline_component  # , needs_tokenized_input
from eea.corpus.utils import corpus_base_path
from eea.corpus.utils import hashed_id
from gensim.models.phrases import Phrases
from itertools import tee, chain
from pyramid.threadlocal import get_current_request
from rq.decorators import job
from rq.registry import StartedJobRegistry
from textacy.doc import Doc
import logging
import os.path


logger = logging.getLogger('eea.corpus')


class PhraseFinderWidget(MappingWidget):
    """ Mapping widget with custom template
    """

    template = 'phrase_form'

    def get_template_values(self, field, cstruct, kw):
        values = super(PhraseFinderWidget, self).get_template_values(
            field, cstruct, kw)

        values['job_status'] = 'preview_not_available'

        req = get_current_request()

        pstruct = req.create_corpus_pipeline_struct.copy()
        pstruct.pop('preview_mode')
        phash_id = phrase_model_id(**pstruct)

        base_path = corpus_base_path(pstruct['file_name'])
        cache_path = os.path.join(base_path, '%s.phras' % phash_id)

        # look for an already existing model
        if os.path.exists(cache_path):
            values['job_status'] = 'preview_available'
            return values

        # look for a job created for this model
        for jb in queue.get_jobs():
            if jb.meta['phrase_model_id'] == phash_id:
                values['job_status'] = 'preview_' + jb.get_status()
                return values

        return values


class PhraseFinder(Schema):
    """ Schema for the phrases finder
    """

    widget = PhraseFinderWidget()       # overrides the default template
    description = "Find and process phrases in text."


@pipeline_component(schema=PhraseFinder,
                    title="Find and process phrases")
def process(content, env, **settings):       # pipeline, preview_mode,
    """ We can run in several modes:

    * remove all text but leave the phrases tokens
    * append the collocations to the document
    * replace the collocations where they appear

    TODO: implement the above
    """

    pipeline = env['pipeline'][:env['position']+1]
    file_name = env['file_name']
    text_column = env['text_column']

    # convert content stream to textacy docs
    content = (isinstance(doc, str) and Doc(doc) or doc for doc in content)
    content = (doc for doc in content if doc.lang == 'en')

    pid = phrase_model_id(file_name, text_column, pipeline)
    base_path = corpus_base_path(env['file_name'])
    cache_path = os.path.join(base_path, '%s.phras' % pid)

    if os.path.exists(cache_path):
        phrases = Phrases()
        phrases = phrases.load(cache_path)

        for doc in content:
            for sentence in phrases[doc.tokenized_text]:
                yield sentence

        raise StopIteration

    if not env.get('preview_mode'):     # generate the phrases
        cs, ps = tee(content, 2)
        ps = chain.from_iterable(doc.tokenized_text for doc in ps)
        phrases = Phrases(ps)     # TODO: pass settings here
        phrases.save(cache_path)

        for doc in cs:
            for sentence in phrases[doc.tokenized_text]:
                yield sentence

        raise StopIteration

    # if running in preview mode, look for an async job already doing
    # phrase model building

    registry = StartedJobRegistry(queue.name, queue.connection)

    for jid in registry.get_job_ids():
        job = queue.fetch_job()
        pos_pid = job.meta.get('phrase_model_id')

        if pos_pid == pid:
            for doc in content:      # just pass through
                yield doc
            raise StopIteration

    job = queue.enqueue(build_phrases,
                        timeout='1h',
                        args=(
                            pipeline,
                            file_name,
                            text_column,
                        ),
                        meta={'phrase_model_id': pid},
                        kwargs={})
    print(job.id)
    for doc in content:      # just pass through
        yield doc


@job(queue=queue)
def build_phrases(pipeline, file_name, text_column, **kw):
    """ Async job to build a corpus using the provided pipeline
    """

    content_stream = build_pipeline(file_name, text_column, pipeline,
                                    preview_mode=False)

    pid = phrase_model_id(file_name, text_column, pipeline)

    base_path = corpus_base_path(file_name)
    cache_path = os.path.join(base_path, '%s.phras' % pid)

    logger.info('Creating phrases at %s', cache_path)

    phrases = Phrases(content_stream)     # TODO: pass settings here
    phrases.save(cache_path)


def phrase_model_id(file_name, text_column, pipeline):
    """ Calculate a hash as consistent uid based on the pipeline
    """
    salt = [(file_name, text_column)]
    for name, settings in pipeline:
        if isinstance(settings, dict):
            settings = settings.copy()
            settings.pop('schema_position', None)
            settings.pop('schema_type', None)
            settings = sorted(settings.items())
        salt.append((name, settings))
    return hashed_id(salt)
