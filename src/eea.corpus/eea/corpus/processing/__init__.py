from collections import namedtuple, OrderedDict
from eea.corpus.utils import upload_location
import colander as c
import deform
import pandas as pd
import venusian


# container for registered pipeline components
pipeline_registry = OrderedDict()

Processor = namedtuple('Processor', ['name', 'klass', 'process', 'title'])


def pipeline_component(schema, title):
    """ Register a processing function as a pipeline component, with a schema

    A pipeline component is two pieces:

    * a ``process(content, **kwargs)`` function that performs any needed
    transformation on the input content.
    * a schema that will provide the necessary parameters values for the
    ``register`` function call

    Use such as:

        class SomeSettingsSchema(colander.Schema):
            count = colander.SchemaNode(colander.Int)

        @pipeline_component(schema=SomeSettingsSchema, title='Generic Pipe')
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

        def callback(scanner, name, func):
            print(scanner, name, func)

            name = '_'.join((func.__module__, name)).replace('.', '_')

            class WrappedSchema(schema):
                schema_type = c.SchemaNode(
                    c.String(),
                    widget=deform.widget.HiddenWidget(),
                    default=name,
                    missing=name,
                )
                schema_position = c.SchemaNode(
                    c.Int(),
                    widget=deform.widget.HiddenWidget(),
                    default=-1,
                    missing=-1,
                )

            p = Processor(name, WrappedSchema, func, title)
            pipeline_registry[name] = p
            print('Registered', p)

            return func

        venusian.attach(process, callback)
        return process

    return decorator


def build_pipeline(file_name, text_column, pipeline):
    """ Runs file through pipeline and returns result

    A pipeline component:

    * should read a stream of data
    * should yield a stream of data

    Inside, it has absolute control over the processing. It can either act in
    stream mode, processing incoming data (and yielding "lines" of content) or
    it can read all the input stream and then yield content.

    The yielded content can be statements, documents, etc.

    # TODO: do we need line_count here, to hint to the processor that we don't
    # want full content?
    """
    document_path = upload_location(file_name)
    df = pd.read_csv(document_path)
    content_stream = df[text_column].__iter__()

    for component_name, kwargs in pipeline:
        component = pipeline_registry[component_name]
        process = component.process
        content_stream = process(content_stream, **kwargs)

    return content_stream
