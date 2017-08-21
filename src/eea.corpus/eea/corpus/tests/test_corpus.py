from unittest.mock import patch


class TestCorpus:

    @patch('eea.corpus.corpus.corpus_base_path')
    @patch('eea.corpus.corpus.build_pipeline')
    def test_build_corpus(self, build_pipeline, corpus_base_path, tmpdir):
        from eea.corpus.corpus import build_corpus
        from textacy.doc import Doc
        import json

        path = tmpdir.join('.', 'test.csv')
        path.mkdir()

        build_pipeline.return_value = [Doc('Hello world', metadata={'1': 2})]
        corpus_base_path.return_value = str(path)

        pipeline = []
        corpus_id = 'test'
        file_name = 'test.csv'
        text_column = 'text'
        kw = {'title': 'first corpus', 'description': 'something else'}

        build_corpus(pipeline, corpus_id, file_name, text_column, **kw)

        assert path.join('test_eea.json').exists()
        assert path.join('test_docs.json').exists()
        assert path.join('test_metadatas.json').exists()
        assert path.join('test_info.json').exists()

        with path.join('test_metadatas.json').open() as f:
            meta = json.load(f)
            assert meta == {'1': 2}

        with path.join('test_eea.json').open() as f:
            meta = json.load(f)
            assert meta == {
                'description': 'something else',
                'title': 'first corpus',
                'statistics': {
                    'docs': 1,
                    'tokens': 2,
                    'sentences': 1,
                    'lang': 'en'
                },
                'kw': {},
                'text_column': 'text'
            }
