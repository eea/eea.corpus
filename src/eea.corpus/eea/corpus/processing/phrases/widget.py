import logging
import os.path

from deform.widget import MappingWidget, default_resource_registry
from pyramid.threadlocal import get_current_request

from eea.corpus.async import get_assigned_job
from eea.corpus.corpus import corpus_base_path
from eea.corpus.processing.utils import (component_phash_id,
                                         get_pipeline_for_component)

logger = logging.getLogger('eea.corpus')


default_resource_registry.set_js_resources(
    'phrase-widget', None, 'eea.corpus:static/phrase-widget.js'
)


class PhraseFinderWidget(MappingWidget):
    """ Mapping widget with custom template

    Template customizations:

        * frame color based on phrase model status
        * the reload button is disabled/enabled based on live phrase model
          status
        * there is an AJAX js script that queries job status and updates the
          widget status indicators (frame color, reload preview button)
    """

    template = 'phrase_form'
    requirements = (('phrase-widget', None),)

    def get_template_values(self, field, cstruct, kw):
        """ Inserts the job status and preview status into template values
        """
        values = super(PhraseFinderWidget, self).\
            get_template_values(field, cstruct, kw)

        values['job_status'] = 'preview_not_available'

        req = get_current_request()

        # TODO: can we refactor this so that we compute the pipeline hash
        # in the pipeline building function?
        pstruct = req.create_corpus_pipeline_struct.copy()
        pstruct['step_id'] = field.schema.name
        phash_id = component_phash_id(
            file_name=pstruct['file_name'],
            text_column=pstruct['text_column'],
            pipeline=get_pipeline_for_component(pstruct)
        )
        values['phash_id'] = phash_id

        logger.info("Phrase widget: need phrase model id %s", phash_id)

        # Calculate the initial "panel status" to assign a status color to this
        # widget
        base_path = corpus_base_path(pstruct['file_name'])
        cpath = os.path.join(base_path, '%s.phras' % phash_id)

        for f in os.listdir(base_path):
            if f.startswith(cpath):    # it's an ngram model
                logger.info("Phrase widget: found a phrase model at %s", cpath)
                values['job_status'] = 'preview_available'

                return values

        # look for a job created for this model
        job = get_assigned_job(phash_id)

        if job is not None:
            values['job_status'] = 'preview_' + job.get_status()

        return values
