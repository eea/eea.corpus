from pyramid import testing
from unittest.mock import sentinel as S, patch


class TestHome:

    @patch('eea.corpus.views.available_documents')
    def test_it(self, available_documents):
        from eea.corpus.views import home

        available_documents.return_value = S.docs

        assert home(None) == {
            'project': 'EEA Corpus Server',
            'documents': S.docs
        }


class TestCreateCorpus:

    @classmethod
    def setup_class(cls):
        cls.config = testing.setUp()
        cls.config.scan('eea.corpus.processing')

    @classmethod
    def teardown_class(cls):
        testing.tearDown()

    def test_apply_schema_edits(self):
        from eea.corpus.processing import pipeline_registry as pr
        from eea.corpus.views import CreateCorpusView

        LimitSchema = pr['eea_corpus_processing_limit_process'].schema
        HTMLSchema = pr['eea_corpus_processing_html_process'].schema
        PrepSchema = pr['eea_corpus_processing_preprocess_process'].schema

        schemas = [
            LimitSchema(name='1st'),
            HTMLSchema(name='2st'),
            PrepSchema(name='3st'),
        ]

        req = testing.DummyRequest()
        view = CreateCorpusView(req)
        res = view._apply_schema_edits(schemas, ['move_up_1st_success'])
        assert [x.name for x in res] == [
            '1st', '2st', '3st'
        ]

        # TODO: finish test
