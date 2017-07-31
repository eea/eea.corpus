from collections import namedtuple, OrderedDict
from eea.corpus.utils import upload_location
from textacy.doc import Doc
import colander as c
import deform
import functools
import pandas as pd
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
                schema_type = c.SchemaNode(
                    c.String(),
                    widget=deform.widget.HiddenWidget(),
                    default=uid,
                    missing=uid,
                )
                schema_position = c.SchemaNode(
                    c.Int(),
                    widget=deform.widget.HiddenWidget(),
                    default=-1,
                    missing=-1,
                )

            p = Processor(uid, WrappedSchema, func, title, actions=[])
            pipeline_registry[uid] = p

            return func

        venusian.attach(process, callback)
        return process

    return decorator


def todoc(doc):
    """ A function that converts any possible input type to a textacy Doc
    """

    if isinstance(doc, Doc):
        return doc

    if isinstance(doc, str):
        return Doc(doc)

    if isinstance(doc, list):
        return Doc(" ".join(list))


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
    df = pd.read_csv(document_path)
    content_stream = df[text_column].__iter__()

    env = {
        'file_name': file_name,
        'text_column': text_column,
        'pipeline': pipeline,
        # Position in pipeline. Allows the processing functions to reconstitute
        # the previous pipeline steps
        'position': 0,
        # True if the pipeline is being previewed
        'preview_mode': preview_mode,
    }

    for i, (component_name, kwargs) in enumerate(pipeline):
        env['position'] = i
        component = pipeline_registry[component_name]
        process = component.process
        content_stream = process(content_stream, env, **kwargs)

    content_stream = (todoc(doc) for doc in content_stream)

    return content_stream


def needs_tokenized_input(func):
    """ A decorator to make sure input stream comes as textacy.Doc objs

    Example:

    @needs_tokenized_input
    def process(content, **settings):
        for doc in content:
            for token in doc:
                print(token)

    # TODO: refactor as a list of convertors that can be passed to processing
    # functions?
    """

    @functools.wraps(func)
    def wrapper(content, **settings):
        for doc in content:
            if isinstance(doc, str):       # doc is list of sentences
                # tokenize using textacy
                # TODO: compare performance, nltk punkt
                yield next(func([Doc(doc).tokenized_text], **settings))
                continue

            yield next(func([doc], **settings))

    return wrapper


def needs_text_input(func):
    """ A decorator to make sure input stream comes as plain strings

    Example:

    @needs_text_input
    def process(content, **settings):
        for doc in content:
            print(doc)
    """

    # TODO: test if content stream yields list of sentences. Convert to text

    @functools.wraps(func)
    def wrapper(content, **settings):
        for doc in content:
            if isinstance(doc, Doc):
                yield next(func([doc.text], **settings))
                continue

            yield next(func([doc], **settings))

    return wrapper
