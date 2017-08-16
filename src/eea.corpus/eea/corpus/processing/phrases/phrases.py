from eea.corpus.utils import to_doc
from gensim.models.phrases import Phrases
from itertools import chain, tee
import logging

logger = logging.getLogger('eea.corpus')


def build_phrase_models(content, base_path, settings):
    """ Build and save the phrase models
    """

    ngram_level = settings['level']

    # According to tee() docs, this may be inefficient in terms of memory.
    # We need to do this because we need multiple passes through the
    # content stream.
    content = chain.from_iterable(doc.tokenized_text for doc in content)
    cs1, cs2 = tee(content, 2)

    for i in range(ngram_level-1):
        phrases = Phrases(cs1)
        path = "%s.%s" % (base_path, i + 2)     # save path as n-gram level
        logger.info("Phrase processor: Saving %s", path)
        phrases.save(path)
        # TODO: gensim complains about not using Phraser(phrases)
        content = phrases[cs2]  # tokenize phrases in content stream
        cs1, cs2 = tee(content, 2)


def use_phrase_models(content, files, settings):

    for doc in content:
        text = doc.tokenized_text
        for fpath in files:
            phrases = Phrases.load(fpath)
            text = phrases[text]

        yield to_doc(". ".join(
            " ".join(sent) for sent in text
        ))

    # TODO: implement filtering modes based on phrases
