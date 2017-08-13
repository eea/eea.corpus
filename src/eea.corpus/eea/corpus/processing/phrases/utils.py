import os


def phrase_model_files(base_path, phash_id):
    """ Returns a list of filenames for this phash_id
    """

    base_name = '%s.phras' % phash_id

    # TODO: test that the phrase model file is "finished"

    files = []
    for name in sorted(os.listdir(base_path)):
        if name.startswith(base_name):
            files.append(os.path.join(base_path, name))

    return files

# cache_path = os.path.join(base_path, )
# base_path, base_name = os.path.split(cache_path)

#
# phash_id = component_phash_id(
#     file_name, text_column, phrase_model_pipeline
# )
# pid = component_phash_id(file_name, text_column, pipeline)
# cache_path = os.path.join(base_path, '%s.phras' % phash_id)
# import os.path
# from eea.corpus.processing.utils import component_phash_id
# job = queue.enqueue(build_phrases,
#                     args=(
#                         phrase_model_pipeline,
#                         file_name,
#                         text_column,
#                         phash_id,
#                         settings,
#                     ),
#                     meta={'phash_id': phash_id},
#                     kwargs={})
