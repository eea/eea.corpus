# from pyramid.paster import setup_logging
from pyramid.paster import bootstrap
from redis import Redis
from rq import Queue, Worker, Connection
from urllib import parse
import click
import os


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
    # TODO: import spacy's model to share it between workers
    pyramid_env = bootstrap(config_uri)
    # Setup logging to allow log output from command methods
    # setup_logging(config_uri)
    try:
        qs = ['default']
        conn = redis_connection()
        with Connection(conn):
            w = Worker(qs)
            w.work()
    finally:
        pyramid_env['closer']()
