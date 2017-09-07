from collections import namedtuple, OrderedDict
from eea.corpus.processing.utils import component_phash_id
from eea.corpus.processing.utils import get_pipeline_for_component
from eea.corpus.utils import upload_location
from itertools import zip_longest
from pandas import read_csv
from textacy.doc import Doc
import colander
import deform
import venusian


# container for registered pipeline components
pipeline_registry = OrderedDict()

Processor = namedtuple('Processor',
                       ['name', 'schema', 'process', 'title', 'actions'])


def pipeline_component(schema, title, actions=None):
    """ Register a processing function as a pipeline component, with a schema

    A pipeline component is two pieces:

    * a ``process(content, **kwargs)`` function that performs any needed
    transformation on the input content.
    * a schema that will provide the necessary parameters values for the
    ``register`` function call

    Additionally, an ``actions`` mapping can be passed, where the keys are
    button names and the values are functions that will handle requests. They
    can be used to handle special cases that can't be foreseen by the main form
    views.

    Use such as:

        class SomeSettingsSchema(colander.Schema):
            count = colander.SchemaNode(colander.Int)

        @pipeline_component(schema=SomeSettingsSchema, title='Generic Pipe',
                            actions={'handle_': handle})
        def process(content, **settings):
            for doc in content:
                # do something
                yield doc

    """

    # This outer function works as a factory for the decorator, to be able to
    # have a closure with parameters for the decorator. The function below is
    # the real decorator

    # The trick of the venusian library is that the decorator, by default,
    # doesn't do anything. It just returns the decorated function. But we
    # register a callback for the venusian scanning process that, once the
    # 'scanning' process is completed (or, rather, the full application is in
    # use), the decorator will start doing what's inside the callback.
    #
    # For the simplified decorator, we just embellish the schema with the
    # required machinery fields, then act as a pass through for the original
    # function

    # TODO: do we really need full venusian to be able to benefit from scan?
    # TODO; can we simplify the schema wrapping process? Inheriance with
    # a "hidden" class seems overkill, but also we don't want to modify, "in
    # place" the original schema, because that can be reused.

    def decorator(process):
        uid = '_'.join((process.__module__,
                        process.__name__)).replace('.', '_')

        def callback(scanner, name, func):

            class WrappedSchema(schema):
                schema_type = colander.SchemaNode(
                    colander.String(),
                    widget=deform.widget.HiddenWidget(),
                    default=uid,
                    missing=uid,
                )

            p = Processor(uid, WrappedSchema, func, title, actions=[])
            pipeline_registry[uid] = p

            return func

        venusian.attach(process, callback)
        return process

    return decorator


def build_pipeline(file_name, text_column, pipeline, preview_mode=True):
    """ Runs file through pipeline and returns result

    A pipeline component:

    * should read a stream of data
    * should yield a stream of data

    Inside, it has absolute control over the processing. It can either act in
    stream mode, processing incoming data (and yielding "lines" of content) or
    it can read all the input stream and then yield content.

    The yielded content can be statements, documents, etc.

    """
    document_path = upload_location(file_name)
    df = read_csv(document_path)

    cs = iter(df[text_column])
    df = df[df.columns.difference([text_column])]
    ms = (dict(zip(df.keys(), row)) for row in df.values)
    stream = zip_longest(cs, ms)

    content_stream = (
        Doc(text, lang='en', metadata=meta) for text, meta in stream
        # strip rows where there's no text
        if text and isinstance(text, str)
    )

    env = {
        'file_name': file_name,
        'text_column': text_column,
        'pipeline': pipeline,

        # True if the pipeline is being previewed
        'preview_mode': preview_mode,

        # schema name of the current step allows processors to reconstitute
        # previous pipeline steps
        'step_id': None,
    }

    for (component_name, step_id, kwargs) in pipeline:
        env['step_id'] = step_id

        # calculate a hash of current + previous pipeline steps
        # this pottentially allows processors to use a cache, if needed
        step_pipeline = get_pipeline_for_component(env)
        phash_id = component_phash_id(
            file_name, text_column, step_pipeline
        )
        env['phash_id'] = phash_id

        component = pipeline_registry[component_name]
        process = component.process
        content_stream = process(content_stream, env, **kwargs)

    return content_stream


def includeme(config):      # pragma: no cover
    config.include('.phrases')
