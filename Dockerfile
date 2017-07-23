FROM floydhub/python-base:latest-gpu-py3

MAINTAINER "Antonio De Marinis" <demarinis@eea.europa.eu>

# Install essential dependencies
RUN pip --no-cache-dir install -U \
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
        beautifulsoup4 \
        unidecode \
        click

RUN pip --no-cache-dir install \
        cld2-cffi \
        pyldavis \
        phrasemachine \
        textacy \
        wordcloud

# # convert phrasemachine to python3 code
RUN cd /usr/local/lib/python3.5/site-packages/phrasemachine \
        && 2to3 -w *.py

RUN python -m spacy.en.download all

RUN pip --no-cache-dir install \
        gensim

# COPY corpus /corpus
RUN mkdir /corpus

ADD ./src /src

# RUN pip install -r /src/eea.corpus/requirements.txt

RUN cd /src/eea.corpus \
      && python setup.py develop

WORKDIR /src/eea.corpus

CMD python setup.py develop && pserve /src/eea.corpus/development.ini

EXPOSE 6543
