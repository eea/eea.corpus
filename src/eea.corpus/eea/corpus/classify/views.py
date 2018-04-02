import pyramid.httpexceptions as exc
from pyramid.view import view_config
from pyramid_deform import FormView

from eea.corpus.corpus import get_corpus
from eea.corpus.schema import ClassifficationModelSchema


# from eea.corpus.utils import tokenizer


@view_config(route_name="corpus_classify",
             renderer='eea.corpus:templates/classify.pt')
class CreateClassificationModelView(FormView):
    schema = ClassifficationModelSchema()
    buttons = ('classify', 'fasttext')

    score = None

    def corpus(self):
        """ Return a corpus based on environment.

        It will try to return it from cache, otherwise load it from disk.
        If corpus hasn't been extracted from the document, it will redirect to
        a corpus creation tool.
        """

        corpus = get_corpus(self.request)

        if corpus is None:
            raise exc.HTTPNotFound()

        return corpus

    def metadata(self):
        """ Show metadata about context document
        """
        # TODO: show info about processing and column
        corpus = self.corpus()

        return {
            'docs': corpus.n_docs,
            'sentences': corpus.n_sents,
            'tokens': corpus.n_tokens,
            'lang': corpus.lang,
        }

    def classify_success(self, appstruct):
        corpus = self.corpus()

    def fasttext_success(self, appstruct):
        from itertools import islice
        # from pyfasttext import FastText

        corpus = self.corpus()
        docs = [doc for doc in corpus
                if not isinstance(doc['metadata']['Category Path'], float)]

        split = int(corpus.n_docs * 0.9)        # TODO: should be docs

        train_docs = islice(docs, 0, split)
        test_docs = islice(docs, split, corpus.n_docs)

        print('Writing corpus to disk')
        lines = []

        for doc in train_docs:
            labels = doc['metadata']['Category Path'].replace('/', ' __label__')
            labels = labels.strip()
            # labels = '__label__'+doc.metadata['Category Path'].split('/')[1]
            text = doc['text'].replace('\n', ' ')
            line = " ".join([labels, text])
            lines.append(line)

        import unicodedata
        with open('/tmp/corpus-train.txt', 'wb') as f:
            s = '\n'.join(lines)
            s = unicodedata.normalize('NFKD', s).encode('ascii', 'ignore')
            f.write(s)

        y_test = []
        test_lines = []
        with open('/tmp/corpus-test.txt', 'w') as f:
            for doc in test_docs:
                labels = [x for x in doc['metadata']['Category Path'].split('/')
                          if x]
                # labels = '__label__' + \
                #     doc.metadata['Category Path'].split('/')[1]
                test_lines.append(doc['text'].replace('\n', ' '))
                y_test.append(labels)
            f.write('\n'.join(test_lines))

        print("Training model")
        # model = fasttext.supervised()
        import fasttext as ft
        model = ft.supervised(input_file='/tmp/corpus-train.txt',
                              output='/tmp/ftmodel', epoch=100)
        print("Model trained")

        # from sklearn import metrics
        # self.score = metrics.accuracy_score(y_test, pred)

        pred = model.predict(test_lines, k=2)
        zz = list(zip(pred, y_test))
        tt = [x for x in zz if set(x[0]) != set(x[1])]
        notok = len(tt)
        self.score = notok * 100 / len(zz)
        print("Score %s" % self.score)

        # xx = model.predict_proba(test_lines, k=2)
