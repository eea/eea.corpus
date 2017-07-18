from collections import namedtuple, OrderedDict
from eea.corpus.utils import upload_location
import colander as c
import deform
import pandas as pd


# container for registered pipeline components
pipeline_registry = OrderedDict()

Processor = namedtuple('Processor', ['name', 'klass', 'process', 'title'])


def register_pipeline_component(schema, process, title):
    """ Call this to register a new pipeline component.

    A pipeline component is two pieces:

    * a ``process(content, **kwargs)`` function that performs any needed
    transformation on the input content.
    * a schema that will provide the necessary parameters values for the
    ``register`` function call
    """
    # TODO: is it possible to avoid wrapping schema?
    name = (process.__module__ + '.' + process.__qualname__).replace('.', '_')

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

    p = Processor(name, WrappedSchema, process, title)
    pipeline_registry[name] = p


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

    for process, kwargs in pipeline:
        content_stream = process(content_stream, **kwargs)

    return content_stream
