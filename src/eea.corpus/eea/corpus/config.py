import os

CORPUS_STORAGE = "/corpus"


def upload_location(file_name):
    """ Returns the path where an upload file would be saved, in the storage
    """

    assert not file_name.startswith('.')

    return os.path.join(CORPUS_STORAGE, file_name)
