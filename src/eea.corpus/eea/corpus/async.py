from flask import Flask
from pkg_resources import resource_filename
from pyramid.paster import bootstrap
from redis import Redis
from redis.exceptions import ConnectionError
from rq import Queue, Worker, Connection
from rq.registry import StartedJobRegistry
from urllib import parse
import click
import logging
import os
import rq_dashboard


logger = logging.getLogger('eea.corpus')


def redis_connection():
    redis_uri = os.environ.get('REDIS_URL', 'redis://localhost:6379/0')
    p = parse.urlparse(redis_uri)
    host, port = p.netloc.split(':')
    db = len(p.path) > 1 and p.path[1:] or '0'
    conn = Redis(host=host, port=port, db=db)
    return conn


def make_queue(name='default'):
    queue = Queue(connection=redis_connection())
    return queue


queue = make_queue()


@click.command()
@click.argument('config_uri')
def worker(config_uri):
    """ Console entry script that starts a worker process
    """
    # TODO: import spacy's model to share it between workers

    pyramid_env = bootstrap(config_uri)

    # this conflicts with normal worker output
    # TODO: solve logging for the console
    # Setup logging to allow log output from command methods
    # from pyramid.paster import setup_logging
    # setup_logging(config_uri)

    try:
        qs = ['default']
        conn = redis_connection()
        with Connection(conn):
            w = Worker(qs)
            w.work()
    finally:
        pyramid_env['closer']()


def dashboard(global_config, **settings):
    """ WSGI entry point for the Flask app RQ Dashboard
    """

    redis_uri = os.environ.get('REDIS_URL', 'redis://localhost:6379/0')
    p = parse.urlparse(redis_uri)
    host, port = p.netloc.split(':')
    db = len(p.path) > 1 and p.path[1:] or '0'

    redis_settings = {
        'REDIS_URL': redis_uri,
        'REDIS_DB': db,
        'REDIS_HOST': host,
        'REDIS_PORT': port,
    }

    app = Flask(__name__,
                static_url_path="/static",
                static_folder=resource_filename("rq_dashboard", "static")
                )
    app.config.from_object(rq_dashboard.default_settings)
    app.config.update(redis_settings)
    app.register_blueprint(rq_dashboard.blueprint)
    return app.wsgi_app


def get_assigned_job(phash_id):
    """ Get the queued or processing job for this pipeline component processor

    TODO: look into more registries
    """

    # First, look for an already started job
    registry = StartedJobRegistry(queue.name, queue.connection)
    try:
        jids = registry.get_job_ids()
    except ConnectionError:
        logger.warning("ConnectionError, could not get a list of job ids")
        jids = []

    for jid in jids:
        job = queue.fetch_job(jid)
        if phash_id == job.meta.get('phash_id'):
            logger.info("Async job found %s", job.id)
            return job

    # Look for a queued job
    try:
        jobs = queue.get_jobs()
    except ConnectionError:
        logger.warning("ConnectionError, could not get a list of jobs")
    jobs = []
    for job in jobs:  # look for a job created for this model
        if job.meta['phash_id'] == phash_id:
            logger.info("Async job found %s", job.id)
            return job
