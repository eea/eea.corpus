# EEA Corpus (alpha stage)

This docker image is based on spaCy, Textacy, pyLDAvis & others to analyse the
EEA Corpus (the collection of all published EEA documents) or any other CSV
file with a column of text.

It provides a number of Machine Learning and Natural Language Processing
algorthims that can be run on top of the EEA Corpus or a subset of it.

~The idea is to provide these methods over a REST API when possible.~

## Current features

### Compose a text transformation pipeline to prepare a corpus

First upload a CSV file, then use the "Create a corpus" button to enter
the pipeline composition page.

### Create and visualise topic models via pyLDAvis.

The topics are found via a text-mining technique called
[Topic Modeling](https://en.wikipedia.org/wiki/Topic_model).

In machine learning and natural language processing, a topic model is a
type of statistical model for discovering the abstract "topics" that occur in a
collection of documents.

[Video demonstration](https://www.youtube.com/watch?v=IksL96ls4o0&t=255s)

![LDA visualisation example](ldavis.png?raw=true "LDA visualisation example")

## How to run:

```
docker-compose build
docker-compose up -d
```

This will (after some time) start the EEA Corpus application server on
[localhost:8181](http://0.0.0.0:8181)

## EEA Corpus Data

The latest EEA Corpus dataset can be produced by visiting [global
catalogue](http://search.apps.eea.europa.eu/)  > See all results > download
csv.

Once the csv file is downloaded, you can pass it to this application to be
analysed. Make sure your first column is the "document text" to be analysed.
The other columns are considered metadata.

You may download an already generated large EEA corpus data for testing like
this:

```
curl -L -o data.csv https://www.dropbox.com/s/sihmoc4wwpl0kr2/data_all.csv?dl=1
```
