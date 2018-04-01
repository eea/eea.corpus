class TestPreprocess:
    def test_schema(self):
        from eea.corpus.processing.preprocess import TextacyPreprocess
        assert len(TextacyPreprocess().children) == 11

    def test_from_doc(self, doc_content_stream):
        from eea.corpus.processing.preprocess import process

        stream = process(doc_content_stream, {})

        doc = next(stream)
        assert isinstance(doc, dict)
        assert doc['text'].startswith(
            'assessment-2 Use of freshwater resources')
