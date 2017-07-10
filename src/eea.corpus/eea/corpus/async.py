from redis import Redis
from rq import Queue, Worker, Connection
from urllib import parse
import os
import sys


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


def worker():
    # TODO: import spacy's model to share it between workers
    qs = sys.argv[1:] or ['default']
    conn = redis_connection()
    with Connection(conn):
        w = Worker(qs)
        w.work()
