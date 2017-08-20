TEXT = """assessment-2 Use of freshwater resources In general, renewable water
is abundant in Europe. However, signals from long-term climate and hydrological
assessments, including on population dynamics, indicate that there was a 24
% decrease in renewable water resources per capita across Europe between 1960
and 2010, particularly in southern Europe. The densely populated river basins
in different parts of Europe, which correspond to 11 % of the total area of
Europe, continue to be hotspots for water stress conditions, and, in the summer
of 2014, there were 86 million inhabitants inthese areas. Around 40 % of the
inhabitants in the Mediterranean region lived under water stress conditions in
the summer of 2014. Groundwater resources and rivers continue to be affected
by overexploitation in many parts of Europe, especially in the western and
eastern European basins. A positive development is that water abstraction
decreased by around 7 %between 2002 and 2014. Agriculture is still the main
pressure on renewable water resources. In the spring of 2014, this sector used
66 % of the total water used in Europe. Around 80 % of total water abstraction
for agriculture occurred in the Mediterranean region. The total irrigated area
in southern Europe increased by 12 % between 2002 and 2014, but the total
harvested agricultural production decreased by 36 % in the same period in this
region.   On average, water supply for households per capita is around 102
L/person per day in Europe, which means that there is 'no water stress'.
However, water scarcity conditions created by population growth and
urbanisation, including tourism, have particularly affected small Mediterranean
islands and highly populated areas in recent years. Because of the huge
volumes of water abstracted for hydropower and cooling, the hydromorphology and
natural hydrological regimes of rivers and lakes continue to be altered. The
targets set in the water scarcity roadmap, as well as the key objectives of the
Seventh Environment Action Programme in the context of water quantity, were not
achieved in Europe for the years 20022014. 'wei' (water exploitation index)
water abstraction  CSI CSI018 WAT WAT001 018 001
"""


class TestNounChunks:
    def make_one(self, mode):
        from eea.corpus.processing.noun_chunks import process
        from textacy.doc import Doc

        doc = Doc(TEXT)

        settings = {'mode': mode}
        stream = process([doc], {}, **settings)

        return next(stream)

    def test_tokenize(self):
        res = self.make_one('tokenize')

        assert 'renewable_water' in res.text
        assert 'renewable water' not in res.text

        assert 'A_positive_development' in res.text
        assert 'A positive development' not in res.text

        assert 'In general' in res.text

    def test_append(self,):
        res = self.make_one('append')

        assert 'renewable_water' in res.text
        assert 'renewable water' in res.text

        assert 'A_positive_development' in res.text
        assert 'A positive development' in res.text

        assert 'In general' in res.text

    def test_replace(self,):
        res = self.make_one('replace')

        assert 'renewable_water' in res.text
        assert 'renewable water' not in res.text

        assert 'A_positive_development' in res.text
        assert 'A positive development' not in res.text

        assert 'In general' not in res.text

    def test_schema(self):
        from eea.corpus.processing.noun_chunks import NounChunks
        assert len(NounChunks().children) == 1
