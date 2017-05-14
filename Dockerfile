FROM floydhub/spacy:latest
MAINTAINER "Antonio De Marinis" <demarinis@eea.europa.eu>

# Install essential dependencies
RUN pip --no-cache-dir install \
        backports.csv \
        cachetools \
        cytoolz \
        ftfy \
        fuzzywuzzy \
        ijson \
        matplotlib \
        networkx \
        numpy \
        pyemd \
        pyphen \
        python-levenshtein \
        requests \
        scipy \
        scikit-learn \
        spacy \
        beautifulsoup4 \
        unidecode

# Download best-matching default spacy english model
# RUN python -m spacy download en
RUN python -m spacy.en.download all

RUN pip --no-cache-dir install \
        cld2-cffi \
        pyldavis \
        textacy

RUN pip --no-cache-dir install \
        phrasemachine

# convert phrasemachine to python3 code
RUN cd /usr/local/lib/python3.6/site-packages/phrasemachine \
        && 2to3 -w *.py

RUN pip --no-cache-dir install click

COPY corpus /corpus

WORKDIR /corpus

EXPOSE 8888
