FROM floydhub/spacy:latest
MAINTAINER "Antonio De Marinis" <demarinis@eea.europa.eu>

# Install dependencies
RUN pip --no-cache-dir install \
        backports.csv \
        cachetools \
        cld2-cffi \
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
        unidecode \
        pyldavis \
        textacy
        
# Download best-matching default spacy english model
# RUN python -m spacy download en
RUN python -m spacy.en.download all

COPY corpus /corpus

WORKDIR /corpus

EXPOSE 8888
