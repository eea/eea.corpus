from unittest.mock import patch, sentinel as S, Mock    # , call,
import pytest


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

    @patch('eea.corpus.processing.phrases.async.Phrases')
    def test_build_phrase_models(self, Phrases):
        from eea.corpus.processing.phrases.async import build_phrase_models

        content = ['hello', 'world']

        phrases = Phrases()
        Phrases.return_value = phrases

        build_phrase_models(content, '/corpus/some.csv.phras', {'level': 2})

        # call count should be 1, but we called above once
        assert Phrases.call_count == 2
        assert phrases.save.call_args[0] == ('/corpus/some.csv.phras.2',)

        build_phrase_models(content, '/corpus/some.csv.phras', {'level': 3})

        # call count should be 1, but it accumulates with the 2 above
        assert Phrases.call_count == 4
        assert phrases.save.call_args[0] == ('/corpus/some.csv.phras.3',)

    @pytest.mark.slow
    def test_build_phrase_models_real(self, simple_content_stream):

        from eea.corpus.processing.phrases.async import build_phrase_models
        from eea.corpus.utils import rand
        from gensim.models.phrases import Phrases
        from itertools import tee, chain
        import os.path
        import tempfile

        content_A, content_B, test_A = tee(simple_content_stream, 3)

        # proof that the simple_content_stream can be used for phrases
        # ph_model = Phrases(content_A)
        # phrases = list(ph_model.export_phrases(sents))
        # assert phrases[0][0].decode('utf-8') == 'freshwater resources'

        base_dir = tempfile.gettempdir()
        b_name = rand(10)
        base_path = os.path.join(base_dir, b_name)
        build_phrase_models(content_A, base_path, {'level': 2})

        assert b_name + '.2' in os.listdir(base_dir)
        assert not (b_name + '.3' in os.listdir(base_dir))
        os.remove(base_path + '.2')

        t_name = rand(10)
        base_path = os.path.join(base_dir, t_name)
        build_phrase_models(content_B, base_path, {'level': 3})

        assert t_name + '.2' in os.listdir(base_dir)
        assert t_name + '.3' in os.listdir(base_dir)

        pm2 = Phrases.load(base_path + '.2')
        pm3 = Phrases.load(base_path + '.3')

        os.remove(base_path + '.2')
        os.remove(base_path + '.3')

        # an iterator of sentences, each a list of words
        trigrams = pm3[pm2[test_A]]
        words = chain.from_iterable(trigrams)
        w2, w3 = tee(words, 2)

        bigrams = [w for w in w2 if w.count('_') == 1]
        assert len(bigrams) == 19829
        assert len(set(bigrams)) == 1287

        trigrams = [w for w in w3 if w.count('_') == 2]
        assert len(trigrams) == 4468
        assert len(set(trigrams)) == 335

        assert bigrams[0] == 'freshwater_resources'
        assert trigrams[0] == 'water_stress_conditions'


class TestProcess:

    @patch('eea.corpus.processing.phrases.process.corpus_base_path')
    def test_cached_phrases_no_files(self,
                                     corpus_base_path,
                                     doc_content_stream):
        from eea.corpus.processing.phrases.process import cached_phrases
        from pkg_resources import resource_filename

        base_path = resource_filename('eea.corpus', 'tests/fixtures/')
        corpus_base_path.return_value = base_path

        # we want the B.phras.* files in fixtures
        env = {'phash_id': 'X', 'file_name': 'ignore'}
        settings = {}

        stream = cached_phrases(doc_content_stream, env, settings)
        with pytest.raises(StopIteration):
            next(stream)

    @pytest.mark.slow
    @patch('eea.corpus.processing.phrases.process.corpus_base_path')
    def test_cached_phrases_cached_files(self,
                                         corpus_base_path,
                                         doc_content_stream):

        from eea.corpus.processing.phrases.process import cached_phrases
        from itertools import islice
        from pkg_resources import resource_filename

        base_path = resource_filename('eea.corpus', 'tests/fixtures/')
        corpus_base_path.return_value = base_path

        # we want the B.phras.* files in fixtures
        env = {'phash_id': 'B', 'file_name': 'ignore'}
        settings = {}

        stream = cached_phrases(doc_content_stream, env, settings)
        docs = list(islice(stream, 0, 4))
        assert 'indicate_that there_was a ' in docs[1]
        assert 'water_resources per_capita across' in docs[1]
        assert "under water_stress_conditions in the" in docs[3]

    @patch('eea.corpus.processing.phrases.process.produce_phrases')
    @patch('eea.corpus.processing.phrases.process.cached_phrases')
    def test_process_yield_from_cache(self,
                                      cached_phrases,
                                      produce_phrases,
                                      simple_content_stream):

        from eea.corpus.processing.phrases.process import process

        cached_phrases.return_value = ['hello', 'world']
        env = {'preview_mode': False}

        res = list(process(simple_content_stream, env))

        assert cached_phrases.call_count == 1
        assert produce_phrases.call_count == 1
        assert res == ['hello', 'world']

    @patch('eea.corpus.processing.phrases.process.preview_phrases')
    @patch('eea.corpus.processing.phrases.process.produce_phrases')
    @patch('eea.corpus.processing.phrases.process.cached_phrases')
    def test_process_yield_from_preview(self,
                                        cached_phrases,
                                        produce_phrases,
                                        preview_phrases,
                                        simple_content_stream):

        from eea.corpus.processing.phrases.process import process

        cached_phrases.return_value = []
        preview_phrases.return_value = ['from', 'preview']
        env = {'preview_mode': True}

        res = list(process(simple_content_stream, env))

        assert cached_phrases.call_count == 1
        assert preview_phrases.call_count == 1
        assert produce_phrases.call_count == 0
        assert res == ['from', 'preview']

    @patch('eea.corpus.processing.phrases.process.preview_phrases')
    @patch('eea.corpus.processing.phrases.process.produce_phrases')
    @patch('eea.corpus.processing.phrases.process.cached_phrases')
    def test_process_yield_from_produce(self,
                                        cached_phrases,
                                        produce_phrases,
                                        preview_phrases,
                                        simple_content_stream):

        from eea.corpus.processing.phrases.process import process

        cached_phrases.return_value = []
        produce_phrases.return_value = ['from', 'produce']
        env = {'preview_mode': False}

        res = list(process(simple_content_stream, env))

        assert cached_phrases.call_count == 1
        assert preview_phrases.call_count == 0
        assert produce_phrases.call_count == 1
        assert res == ['from', 'produce']

    @patch('eea.corpus.processing.phrases.process.corpus_base_path')
    def test_preview_phrases_with_cache_files(self, corpus_base_path):
        from eea.corpus.processing.phrases.process import preview_phrases
        from pkg_resources import resource_filename

        base_path = resource_filename('eea.corpus', 'tests/fixtures/')
        corpus_base_path.return_value = base_path

        content = ['hello', 'world']
        env = {
            'file_name': 'x.csv',
            'text_column': 'text',
            'phash_id': 'B',
        }

        stream = preview_phrases(content, env, {})
        assert list(stream) == []

    @patch('eea.corpus.processing.phrases.process.get_assigned_job')
    @patch('eea.corpus.processing.phrases.process.corpus_base_path')
    def test_preview_phrases_nocache_files_with_job(self,
                                                    corpus_base_path,
                                                    get_assigned_job):
        from eea.corpus.processing.phrases.process import preview_phrases
        from pkg_resources import resource_filename

        get_assigned_job.return_value = Mock(id='job1')
        base_path = resource_filename('eea.corpus', 'tests/fixtures/')
        corpus_base_path.return_value = base_path

        content = ['hello', 'world']
        env = {
            'file_name': 'x.csv',
            'text_column': 'text',
            'phash_id': 'X',
        }

        stream = preview_phrases(content, env, {})
        assert list(stream) == ['hello', 'world']

    @patch('eea.corpus.processing.phrases.process.build_phrases')
    @patch('eea.corpus.processing.phrases.process.get_pipeline_for_component')
    @patch('eea.corpus.processing.phrases.process.get_assigned_job')
    @patch('eea.corpus.processing.phrases.process.corpus_base_path')
    def test_preview_phrases_nocache_files_sched_job(
        self, corpus_base_path, get_assigned_job, get_pipeline_for_component,
        build_phrases
    ):
        from eea.corpus.processing.phrases.process import preview_phrases
        from pkg_resources import resource_filename

        get_assigned_job.return_value = None
        base_path = resource_filename('eea.corpus', 'tests/fixtures/')
        corpus_base_path.return_value = base_path

        content = ['hello', 'world']
        env = {
            'file_name': 'x.csv',
            'text_column': 'text',
            'phash_id': 'X',
        }

        stream = preview_phrases(content, env, {})
        assert list(stream) == ['hello', 'world']

        assert build_phrases.delay.call_count == 1

    @patch('eea.corpus.processing.phrases.process.corpus_base_path')
    @patch('eea.corpus.processing.phrases.process.get_assigned_job')
    @patch('eea.corpus.processing.phrases.process.cached_phrases')
    def test_produce_phrases_with_cached_files(self,
                                               cached_phrases,
                                               get_assigned_job,
                                               corpus_base_path):
        from eea.corpus.processing.phrases.process import produce_phrases
        from pkg_resources import resource_filename

        content = ['hello', 'world']
        env = {'phash_id': 'B', 'file_name': 'x.csv', 'text_column': 'text'}
        base_path = resource_filename('eea.corpus', 'tests/fixtures/')

        corpus_base_path.return_value = base_path
        cached_phrases.return_value = ['something', 'else']

        stream = produce_phrases(content, env, {})

        assert list(stream) == []
        assert corpus_base_path.call_count == 1
        assert get_assigned_job.call_count == 0

    @patch('eea.corpus.processing.phrases.process.build_phrases')
    @patch('eea.corpus.processing.phrases.process.get_pipeline_for_component')
    @patch('eea.corpus.processing.phrases.process.corpus_base_path')
    @patch('eea.corpus.processing.phrases.process.get_assigned_job')
    @patch('eea.corpus.processing.phrases.process.cached_phrases')
    def test_produce_phrases_with_no_job(self,
                                         cached_phrases,
                                         get_assigned_job,
                                         corpus_base_path,
                                         get_pipeline_for_component,
                                         build_phrases
                                         ):
        from eea.corpus.processing.phrases.process import produce_phrases
        from pkg_resources import resource_filename

        content = ['hello', 'world']
        env = {'phash_id': 'X', 'file_name': 'x.csv', 'text_column': 'text'}
        base_path = resource_filename('eea.corpus', 'tests/fixtures/')

        corpus_base_path.return_value = base_path
        cached_phrases.return_value = ['something', 'else']

        get_assigned_job.return_value = None
        stream = produce_phrases(content, env, {})

        assert list(stream) == ['something', 'else']
        assert corpus_base_path.call_count == 1
        assert get_assigned_job.call_count == 1
        assert get_pipeline_for_component.call_count == 1
        assert build_phrases.call_count == 1
        assert cached_phrases.call_count == 1
