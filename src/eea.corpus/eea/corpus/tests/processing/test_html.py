from unittest.mock import patch


class TestHTML:
    texts = (
        "<strong>Hello</strong> world",
        "Just plain text",
    )

    def test_schema(self):
        from eea.corpus.processing.html import BeautifulSoupText
        assert len(BeautifulSoupText().children) == 0

    def test_clean_text(self):
        from eea.corpus.processing.html import process

        content = process(self.texts, {})

        assert next(content).text == 'Hello world'
        assert next(content).text == 'Just plain text'

    def test_clean_docs(self):
        from eea.corpus.processing.html import process
        from textacy.doc import Doc

        content = (Doc(s) for s in self.texts)
        content = process(content, {})

        assert next(content).text == 'Hello world'
        assert next(content).text == 'Just plain text'

    def test_from_text(self, text_column_stream):
        from eea.corpus.processing.html import process
        from textacy.doc import Doc

        stream = process(text_column_stream, {})

        doc = next(stream)
        assert isinstance(doc, Doc)
        assert doc.text.startswith('assessment-2  Use of freshwater resources')

    def test_from_doc(self, doc_content_stream):
        from eea.corpus.processing.html import process
        from textacy.doc import Doc

        stream = process(doc_content_stream, {})

        doc = next(stream)
        assert isinstance(doc, Doc)
        assert doc.text.startswith('assessment-2  Use of freshwater resources')

    @patch('eea.corpus.processing.html.to_doc')
    def test_to_doc_with_error(self, to_doc):
        from eea.corpus.processing.html import process

        to_doc.side_effect = ValueError()

        stream = process(['hello', 'world'], {})
        assert list(stream) == []
