from unittest.mock import patch


class TestCorpus:

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

    @patch('eea.corpus.corpus.corpus_base_path')
    def test_load_corpus(self, corpus_base_path):
        from pkg_resources import resource_filename
        from eea.corpus.corpus import load_corpus

        base_path = resource_filename('eea.corpus', 'tests/fixtures/')
        corpus_base_path.return_value = base_path

        corpus = load_corpus('test.csv', 'corpusA')

        docs = list(corpus)
        assert len(docs) == 2

        assert docs[0]['text'] == 'Hello world'
        assert docs[0]['metadata'] == {'1': 2}

        assert docs[1]['text'] == 'Second time'
        assert docs[1]['metadata'] == {'3': 4}
