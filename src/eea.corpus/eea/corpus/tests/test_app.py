from pyramid import testing


class TestApp:
    def test_it(self):
        config = testing.setUp()
        settings = {
            'corpus.secret': 'bla'
        }
        from eea.corpus import main
        app = main(config, **settings)

        assert 'home' in app.routes_mapper.routes
        assert 'phrase-model-status' in app.routes_mapper.routes
