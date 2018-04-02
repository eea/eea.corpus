from unittest.mock import patch  # Mock,


class TestCorpus:
    """ Tests for the Corpus class
    """

    @patch('eea.corpus.corpus.io')
    @patch('eea.corpus.corpus.corpus_base_path')
    def test_corpus_caching(self, corpus_base_path, io):
        from eea.corpus.corpus import Corpus

        corpus = Corpus('filename', 'corpusid')
        corpus._docs_stream = iter(range(100))

        assert len(corpus._cache) == 0

        x = list(corpus)
        assert len(x) == 100
        assert len(list(corpus)) == 100
        assert len(list(corpus)) == 100
        assert corpus._use_cache is True
        assert len(corpus._cache) == 100

    @patch('eea.corpus.corpus.io')
    @patch('eea.corpus.corpus.corpus_base_path')
    def test_corpus_metadata(self, corpus_base_path, io):
        from eea.corpus.corpus import Corpus

        corpus = Corpus('a', 'b')
        corpus._meta = {
            'statistics': {'docs': 30},
            'title': 'corpus title',
            'description': 'corpus description',
        }
        assert corpus.n_docs == 30
        assert corpus.title == 'corpus title'
        assert corpus.description == 'corpus description'

    @patch('eea.corpus.corpus.corpus_base_path')
    @patch('eea.corpus.corpus.build_pipeline')
    def test_build_corpus(self, build_pipeline, corpus_base_path, tmpdir):
        from eea.corpus.corpus import build_corpus
        import json

        path = tmpdir.join('.', 'test.csv')
        path.mkdir()

        build_pipeline.return_value = [
            {
                'text': 'Hello world',
                'metadata': {'1': 2}
            },
            {
                'text': 'Second time',
                'metadata': {'3': 4},
            }
        ]
        corpus_base_path.return_value = str(path)

        pipeline = []
        corpus_id = 'test'
        file_name = 'test.csv'
        text_column = 'text'
        kw = {'title': 'first corpus', 'description': 'something else'}

        build_corpus(pipeline, corpus_id, file_name, text_column, **kw)

        assert path.join('test_info.json').exists()
        assert path.join('test_docs.json').exists()

        docs = []
        with path.join('test_docs.json').open() as f:
            for line in f:
                doc = json.loads(line)
                docs.append(doc)

        assert docs[0] == {'text': 'Hello world', 'metadata': {'1': 2}}
        assert docs[1] == {'text': 'Second time', 'metadata': {'3': 4}}
        assert len(docs) == 2

        with path.join('test_info.json').open() as f:
            meta = json.load(f)
            assert meta == {
                'description': 'something else',
                'title': 'first corpus',
                'statistics': {
                    'docs': 2,
                    'lang': 'en'
                },
                'kw': {},
                'text_column': 'text'
            }

    # @patch('eea.corpus.corpus.corpus_base_path')
    # def test_load_corpus(self, corpus_base_path):
    #     from pkg_resources import resource_filename
    #     from eea.corpus.corpus import load_corpus, CORPUS_CACHE
    #
    #     base_path = resource_filename('eea.corpus', 'tests/fixtures/')
    #     corpus_base_path.return_value = base_path
    #
    #     corpus = load_corpus('test.csv', 'corpusA')
    #
    #     docs = list(corpus)
    #     assert len(docs) == 2
    #
    #     assert docs[0]['text'] == 'Hello world'
    #     assert docs[0]['metadata'] == {'1': 2}
    #
    #     assert docs[1]['text'] == 'Second time'
    #     assert docs[1]['metadata'] == {'3': 4}
    #
    #     CORPUS_CACHE.clear()
    # assert doc and corpus_id
    #
    # if corpus_id not in CORPUS_CACHE.get(doc, []):
    #     corpus = Corpus(file_name=doc, corpus_id=corpus_id)
    #
    #     if corpus is None:
    #         return None
    #
    #     CORPUS_CACHE[doc] = {
    #         corpus_id: corpus
    #     }
    #
    # return CORPUS_CACHE[doc][corpus_id]

    # @patch('eea.corpus.corpus.extract_corpus_id')
    # @patch('eea.corpus.corpus.load_corpus')
    # def test_get_corpus(self, load_corpus, extract_corpus_id):
    #     from eea.corpus.corpus import get_corpus    # , CORPUS_CACHE
    #
    #     request = Mock()
    #     corpus = Mock()
    #
    #     extract_corpus_id.return_value = ['doc-a', 'corpus-b']
    #     load_corpus.return_value = None
    #
    #     assert get_corpus(request) is None
    #     assert CORPUS_CACHE == {}
    #
    #     load_corpus.return_value = corpus
    #
    #     res = get_corpus(request)
    #     assert extract_corpus_id.call_count == 2
    #     assert res is corpus
    #
    #     assert 'doc-a' in CORPUS_CACHE
    #     assert CORPUS_CACHE['doc-a']['corpus-b'] is corpus
    #
    #     res = get_corpus(request, 'doc-a', 'corpus-b')
    #     assert extract_corpus_id.call_count == 2
    #     assert res is corpus
    #     assert CORPUS_CACHE['doc-a']['corpus-b'] is corpus
    #
    #     CORPUS_CACHE.clear()
