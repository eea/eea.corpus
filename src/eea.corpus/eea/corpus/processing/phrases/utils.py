import os


def phrase_model_files(base_path, phash_id):
    """ Returns a list of sorted filenames for this phash_id

    The phrase model files are a series of incrementally sufixed numbered files
    """

    base_name = '%s.phras' % phash_id

    # TODO: test that the phrase model file is "finished"

    files = []
    for name in sorted(os.listdir(base_path)):
        if (name != base_name) and name.startswith(base_name):
            files.append(os.path.join(base_path, name))

    return files
