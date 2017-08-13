from eea.corpus.async import queue
from eea.corpus.utils import CORPUS_STORAGE
from glob import iglob
from redis.exceptions import ConnectionError
import logging
import os.path

logger = logging.getLogger('eea.corpus')


def phrase_model_status(request):
    """ A view for information about the async status of a phrase model

    It looks up any existing running or queued async job that would process
    the phrases and returns JSON info about that.
    """

    phash_id = request.matchdict['phash_id']

    # look for a filename in corpus var folder
    fname = phash_id + '.phras'
    glob_path = os.path.join(CORPUS_STORAGE, '**', fname)
    files = list(iglob(glob_path, recursive=True))
    if files:
        return {
            'status': 'OK'
        }

    # TODO: this is the place to flatten all these available statuses
    # statuses: queued,

    try:
        jobs = queue.get_jobs()
    except ConnectionError:
        logger.warning("Phrase model status: could not get job status")
        jobs = []

    for jb in jobs:  # look for a job created for this model
        if jb.meta['phrase_model_id'] == phash_id:
            return {
                'status': 'preview_' + jb.get_status()
            }

    return {
        'status': 'unavailable'
    }
