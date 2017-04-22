#!/bin/sh

VENV=virtualenv-15.1.0/virtualenv.py
URL="https://pypi.python.org/packages/d4/0c/9840c08189e030873387a73b90ada981885010dd9aea134d6de30cd24cb8/virtualenv-15.1.0.tar.gz#md5=44e19f4134906fe2d75124427dc9b716"

curl $URL > /tmp/virtualenv.tgz
tar xzf /tmp/virtualenv.tgz -C ./
/usr/bin/python $VENV --clear ./

rm -rf ./virtualenv*
bin/pip install -U pip

env CC=/usr/bin/gcc-5 bin/pip install cld2-cffi
bin/pip install -r requirements.txt
bin/python -m spacy download en
bin/python load_eea_corpus.py
