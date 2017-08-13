from eea.corpus.processing.phrases.views import phrase_model_status


def includeme(config):
    config.add_route('phrase-model-status', '/phrase-model-status/{phash_id}')
    config.add_view(phrase_model_status, route_name='phrase-model-status',
                    renderer='json')
