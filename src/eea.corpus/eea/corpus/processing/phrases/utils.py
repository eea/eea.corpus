from eea.corpus.async import get_assigned_job
from rq.job import JobStatus as JS
import os
import time


def phrase_model_files(base_path, phash_id):
    """ Returns a list of sorted filenames for this phash_id

    The phrase model files are a series of incrementally sufixed numbered files
    """

    base_name = '%s.phras' % phash_id

    # TODO: test that the phrase model file is "finished"

    files = []
    for name in sorted(os.listdir(base_path)):
        if (name != base_name) and name.startswith(base_name):
            files.append(os.path.join(base_path, name))

    return files


def get_job_finish_status(phash_id, timeout=100):
    """ Wait for the job to finish or abort if job is unable to finish

    Job status can be one of:
        - QUEUED
        - STARTED
        - DEFERRED
        - FINISHED
        - FAILED

    If the job fails to move from queued, deferred to other states, we will
    timeout (return False) after the given timeout period.

    A started job has an infinite timeout period.
    """

    cycle = 0
    os = ''

    while True:
        job = get_assigned_job(phash_id)

        if job is None:
            return False

        st = job.get_status()

        if st != os:
            cycle = 0
            os = st

        if st == JS.FINISHED:       # TODO: should we check cache paths?
            return True

        if st == JS.FAILED:
            return False

        if st == JS.STARTED:        # for started jobs, we wait indefinitely
            cycle = 0

        time.sleep(timeout)     # sleep 10 seconds
        cycle += 10

        if cycle >= timeout:
            break

    return False
