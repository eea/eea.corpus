import pytest


def pytest_addoption(parser):
    parser.addoption("--runslow", action="store_true",
                     default=False, help="run slow tests")


def pytest_collection_modifyitems(config, items):
    if config.getoption("--runslow"):
        # --runslow given in cli: do not skip slow tests
        return
    skip_slow = pytest.mark.skip(reason="need --runslow option to run")
    for item in items:
        if "slow" in item.keywords:
            item.add_marker(skip_slow)


@pytest.fixture
def text_column_stream():
    from pkg_resources import resource_filename
    import pandas as pd

    fpath = resource_filename('eea.corpus', 'tests/fixtures/test.csv')
    df = pd.read_csv(fpath)

    column_stream = iter(df['text'])
    return column_stream


@pytest.fixture
def simple_content_stream(text_column_stream):
    from itertools import chain
    from textacy.doc import Doc

    content = chain.from_iterable(
        Doc(text).tokenized_text for text in text_column_stream
    )

    content = (
        [w.strip().lower() for w in s if w.strip() and w.strip().isalpha()]
        for s in content
    )

    return content


@pytest.fixture
def doc_content_stream(text_column_stream):
    from textacy.doc import Doc
    return (Doc(text) for text in text_column_stream)


# from itertools import chain
# content = chain.from_iterable(
#     Doc(text).tokenized_text for text in text_column_stream
# )
