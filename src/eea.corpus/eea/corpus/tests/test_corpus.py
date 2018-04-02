from unittest.mock import Mock, patch


class TestCorpus:
    """ Tests for the Corpus class
    """

    @patch('eea.corpus.corpus.load_corpus_metadata')
    @patch('eea.corpus.corpus.corpus_base_path')
    def test_corpus_caching(self, corpus_base_path, load_corpus_metadata):
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

    @patch('eea.corpus.corpus.load_corpus_metadata')
    @patch('eea.corpus.corpus.corpus_base_path')
    def test_corpus_metadata(self, corpus_base_path, load_corpus_metadata):
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

    @patch('eea.corpus.corpus.Corpus')
    @patch('eea.corpus.corpus.extract_corpus_id')
    def test_get_corpus(self, extract_corpus_id, Corpus):
        from eea.corpus.corpus import get_corpus

        extract_corpus_id.return_value = ('doc-a', 'corpus-b')
        Corpus.return_value = object()

        request = Mock()
        corpus = get_corpus(request)
        assert corpus is Corpus.return_value
        Corpus.assert_called_with(file_name='doc-a', corpus_id='corpus-b')

        corpus = get_corpus(request, 'doc-b', 'corpus-c')
        assert corpus is Corpus.return_value
        Corpus.assert_called_with(file_name='doc-b', corpus_id='corpus-c')
