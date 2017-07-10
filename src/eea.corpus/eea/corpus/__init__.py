from pyramid.config import Configurator
from pyramid.session import SignedCookieSessionFactory
import matplotlib


matplotlib.use('SVG')       # use SVG backend.


CACHE = {}      # really dummy and simple way to cache corpuses


def corpus_cache(request):
    """ Available as request.corpus_cache.

    Avoids passing the cache around various modules
    """
    return CACHE


def main(global_config, **settings):
    """ This function returns a Pyramid WSGI application.
    """
    secret = settings['corpus.secret']
    session_factory = SignedCookieSessionFactory(secret)

    config = Configurator(settings=settings)
    config.set_session_factory(session_factory)

    config.include('pyramid_chameleon')

    config.add_static_view('static', 'static', cache_max_age=3600)

    config.add_route('home', '/')
    config.add_route('upload_csv', '/upload')
    config.add_route('view_corpus', '/view/{doc}/{corpus}')
    config.add_route('process_csv', '/process/{doc}/')
    config.add_route('view_job', '/view/{doc}/{corpus}/job/{job}')

    config.add_request_method(corpus_cache, reify=True)

    config.scan()

    return config.make_wsgi_app()
