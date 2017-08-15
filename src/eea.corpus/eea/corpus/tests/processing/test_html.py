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
