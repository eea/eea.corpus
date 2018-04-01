TEXT = """def process(content, env, **settings):
    \"\"\" Tokenization
    \"\"\"

    for doc in content:
        text = " ".join(tokenizer(doc.text))

        try:
            yield set_text(doc, text)
        except Exception:
            logger.exception("Error in converting to Doc %r", text)

            continue
"""


class TestRegexTokenizer:
    def test_schema(self):
        from eea.corpus.processing.regextokenizer import RegexTokenizer
        assert len(RegexTokenizer().children) == 1

    def test_from_doc(self):
        from eea.corpus.processing.regextokenizer import process

        pattern = r'[\w\']+|[""!"#$%&\'()*+,-./:;<=>?@[\]^_`{|}~""\\]'

        doc = {'text': TEXT, 'metadata': None}

        res = next(process([doc], {}, regex=pattern))
        assert res['text'].startswith(
            "def process ( content , env , * * settings )"
        )
