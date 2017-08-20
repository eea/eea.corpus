from unittest.mock import patch, Mock


class TestHTML:
    texts = (
        "<strong>Hello</strong> world",
        "Just plain text",
    )

    def test_schema(self):
        from eea.corpus.processing.html import BeautifulSoupText
        assert len(BeautifulSoupText().children) == 0

    def test_clean_docs(self):
        from eea.corpus.processing.html import process
        from textacy.doc import Doc

        content = (Doc(s) for s in self.texts)
        content = process(content, {})

        assert next(content).text == 'Hello world'
        assert next(content).text == 'Just plain text'

    @patch('eea.corpus.processing.html.set_text')
    def test_set_text_with_error(self, set_text):
        from eea.corpus.processing.html import process

        set_text.side_effect = ValueError()
        doc = Mock(text='hello world')

        stream = process([doc], {})
        assert list(stream) == []
