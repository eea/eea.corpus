from eea.corpus.utils import hashed_id


def component_phash_id(file_name, text_column, pipeline):
    """ Calculate a hash as consistent uid based on the pipeline
    """
    salt = [(file_name, text_column)]
    for name, step_id, settings in pipeline:
        if isinstance(settings, dict):
            settings = settings.copy()
            settings.pop('schema_position', None)
            settings.pop('schema_type', None)
            settings = sorted(settings.items())
        salt.append((name, settings))
    return hashed_id(salt)


def get_pipeline_for_component(env):
    """ Retrive all the pipeline steps that influence a processing component

    Normally a component not interested in the whole pipeline, it only needs
    to take input, change input and yield output. In some cases (for example
    the phrase detection), the processing component needs extended information
    about the pipeline steps that come before it:

        * it can compute a hash of its pipeline, for caching purposes.
        * the pipeline is used in the async phrase models generation
    """

    pipeline = []
    for step in env['pipeline']:
        pipeline.append(step)
        if step[1] == env['step_id']:
            break

    return pipeline
