class ClassVocab:
    def __init__(self):
        self.vocab = {}

    def __getitem__(self, k):
        if isinstance(k, float):
            k = 'empty'
        k = [x for x in k.split('/') if x][0]

        if k not in self.vocab:
            x = len(self.vocab)
            self.vocab[k] = x

            return x

        return self.vocab[k]


def train_model(corpus):
    # conventions: X are features, y are labels
    # X_train is array of training feature values,
    # X_test is array with test values
    # y_train are labels for X_train, y_test are labels for X_test

    from sklearn import metrics
    from sklearn.model_selection import train_test_split
    from itertools import tee

    docs = (doc for doc in corpus
            if not isinstance(doc.metadata['Category Path'], float))
    docs_stream, meta_stream = tee(docs, 2)

    print("Transforming docs")
    docs = [doc.text for doc in docs_stream]

    from sklearn.feature_extraction.text import CountVectorizer
    vect = CountVectorizer(input='content', strip_accents='unicode',
                           tokenizer=tokenizer,  # stop_words='english',
                           max_features=5000)

    X = vect.fit_transform(docs)

    from sklearn.feature_extraction.text import TfidfTransformer
    transf = TfidfTransformer()
    X = transf.fit_transform(X)
    # X = X.toarray()   # only needed for GDC

    # from sklearn.feature_extraction.text import TfidfVectorizer
    # vect = TfidfVectorizer(max_features=5000,
    #                        ngram_range=(1, 3), sublinear_tf=True)
    # X = vect.fit_transform(docs)

    # from sklearn.ensemble import RandomForestClassifier
    # model = RandomForestClassifier(n_estimators=100)    # acc: 0.73

    # from sklearn import svm
    # model = svm.SVC(kernel='poly', degree=3, C=1.0)     # acc: 0.66

    # from sklearn.naive_bayes import MultinomialNB       # acc: 0.73
    # model = MultinomialNB(alpha=0.1)        # , fit_prior=True

    # takes a long time, can go higher if more estimators, higher l_rate
    # from sklearn.ensemble import GradientBoostingClassifier   # acc: 0.65
    # model = GradientBoostingClassifier(n_estimators=10,learning_rate=0.1)

    # 0.763 with tfidf from countvect 5000, 0.7 without tfidf
    from sklearn.linear_model import LogisticRegression
    model = LogisticRegression()

    vocab = ClassVocab()
    y = [vocab[doc.metadata['Category Path']] for doc in meta_stream]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.1, random_state=3311)

    print("Training on %s docs" % str(X_train.shape))

    model.fit(X_train, y_train)

    print("Fitting model")
    model.fit(X_train, y_train)
    print("done")

    pred = model.predict(X_test)
    score = metrics.accuracy_score(y_test, pred)
    print(score)
