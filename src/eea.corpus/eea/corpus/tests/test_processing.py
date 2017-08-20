from unittest.mock import patch
from pyramid import testing


class TestProcessing:
    @classmethod
    def setup_class(cls):
        cls.config = testing.setUp()
        cls.config.scan('eea.corpus.processing')

    @classmethod
    def teardown_class(cls):
        testing.tearDown()

    @patch('eea.corpus.processing.upload_location')
    def test_build_pipeline_for_preview(self, upload_location):
        from eea.corpus.processing import build_pipeline
        from pkg_resources import resource_filename

        file_name = 'test.csv'
        upload_location.return_value = resource_filename(
            'eea.corpus', 'tests/fixtures/test.csv')
        text_column = 'text'

        pipeline = [
            ('eea_corpus_processing_limit_process', 'ABC', {'max_count': 2})
        ]

        stream = build_pipeline(file_name, text_column, pipeline,
                                preview_mode=True)

        docs = list(stream)
        assert len(docs) == 2
