from eea.corpus.async import get_assigned_job
from eea.corpus.utils import CORPUS_STORAGE
from glob import iglob
import logging
import os.path

logger = logging.getLogger('eea.corpus')


def phrase_model_status(request):
    """ A view for information about the async status of a phrase model

    It looks up any existing running or queued async job that would process
    the phrases and returns JSON info about that.

    # TODO: this view + template + script implementation still needs work
    """

    phash_id = request.matchdict['phash_id']

    # TODO: when looking for phrase model files, look for lock files as well

    # look for a filename in corpus var folder
    fname = phash_id + '.phras.?'
    glob_path = os.path.join(CORPUS_STORAGE, '**', fname)
    files = list(iglob(glob_path, recursive=True))
    if files:
        return {
            'status': 'OK'
        }

    job = get_assigned_job(phash_id)
    status = job and ('preview_%s' % job.get_status()) or 'unavailable'
    return {
        'status': status
    }
