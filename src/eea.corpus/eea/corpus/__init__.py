from pyramid.config import Configurator
from pyramid.session import SignedCookieSessionFactory


# save image to svg
import matplotlib
matplotlib.use('SVG')


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
    config.add_route('view_csv', '/view/{name}/')
    # config.include('.views')
    config.scan()

    return config.make_wsgi_app()
