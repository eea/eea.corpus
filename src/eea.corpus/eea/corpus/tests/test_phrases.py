from unittest.mock import patch, sentinel as S     # Mock,


class TestSchema:

    def test_it(self):
        from eea.corpus.processing.phrases.schema import PhraseFinder
        from eea.corpus.processing.phrases.widget import PhraseFinderWidget

        schema = PhraseFinder()
        assert isinstance(schema.widget, PhraseFinderWidget)
        assert len(schema.children) == 5


class TestUtils:

    @patch('eea.corpus.processing.phrases.utils.os.listdir')
    def test_phrase_model_files(self, listdir):
        from eea.corpus.processing.phrases.utils import phrase_model_files

        phash_id = 'abc'
        listdir.return_value = [
            'cba',
            'abc.phras.2',
            'abc.phras.1',
            'abc',
            'abc.phras',
        ]
        res = phrase_model_files('/corpus', phash_id)

        assert res == ['/corpus/abc.phras.1', '/corpus/abc.phras.2']


class TestAsync:

    @patch('eea.corpus.processing.phrases.async.build_phrase_models')
    @patch('eea.corpus.processing.phrases.async.build_pipeline')
    @patch('eea.corpus.processing.phrases.async.corpus_base_path')
    def test_build_phrases_job(self, corpus_base_path, build_pipeline,
                               build_phrase_models):

        from eea.corpus.processing.phrases.async import build_phrases

        corpus_base_path.return_value = '/corpus'
        build_pipeline.return_value = S.content

        build_phrases(S.pipeline, 'some.csv', 'text', 'phash_abc', S.settings)

        corpus_base_path.assert_called_once_with('some.csv')

        assert build_pipeline.call_args[0] == ('some.csv', 'text', S.pipeline)
        assert build_phrase_models.call_args[0] == (
            S.content, '/corpus/phash_abc.phras', S.settings
        )
