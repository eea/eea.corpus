class TestStopWords:
    text = """In general, renewable water is abundant in Europe. However,
    signals from long-term climate and hydrological assessments, including on
    population dynamics, indicate that there was a 24% decrease in renewable
    water resources per capita across Europe between 1960 and 2010,
    particularly in southern Europe. The densely populated river basins in
    different parts of Europe, which correspond to 11 % of the total area of
    Europe, continue to be hotspots for water stress conditions, and, in the
    summer of 2014, there were 86 million inhabitants inthese areas. Around 40%
    of the inhabitants in the Mediterranean region lived under water stress
    conditions in the summer of 2014."""

    def test_schema(self):
        from eea.corpus.processing.stopwords import StopWords
        assert len(StopWords().children) == 0

    def test_remove_stopwords(self):
        from eea.corpus.processing.stopwords import process
        from textacy.doc import Doc

        content = process([Doc(self.text)], {})
        text = next(content)

        assert text.n_tokens == 92
        assert 'general' in text.text
        assert 'from' not in text.text
