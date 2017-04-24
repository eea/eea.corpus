#!/bin/sh
# temporary helper script to save ldavis locally
# since pyLDAvis http server is not working within container

wget "http://127.0.0.1:8889/"
wget "http://127.0.0.1:8889/LDAvis.css"
wget "http://127.0.0.1:8889/LDAvis.js"
wget "http://127.0.0.1:8889/d3.js"