from unittest.mock import patch, sentinel as S    # , call, Mock,
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

        # from pkg_resources import resource_filename
        # from textacy.doc import Doc
        # import pandas as pd
        # fpath = resource_filename('eea.corpus', 'tests/fixtures/test.csv')
        # df = pd.read_csv(fpath)
        #
        # column_stream = iter(df['text'])
        # content = chain.from_iterable(
        #     Doc(text).tokenized_text for text in column_stream
        # )
        #
        # content = (
        #   [w.strip().lower() for w in s if w.strip() and w.strip().isalpha()]
        #   for s in content
        # )

        content = simple_content_stream

        content_A, content_B, test_A = tee(content, 3)

        # proof that the content stream can be used for phrases
        # ph_model = Phrases(stream)
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

        # TODO: clean junk from temp folder


class TestProcess:

    # @pytest.mark.slow
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
