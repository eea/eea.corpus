from eea.corpus.utils import to_doc
from eea.corpus.utils import to_text
from textacy.doc import Doc
import unittest

# from pyramid import testing


class ConvertorDecoratorsTests(unittest.TestCase):
    """ Tests for stream conversion decorators found in eea.corpus.utils
    """

    def test_str_to_doc(self):
        res = to_doc('hello world')

        assert isinstance(res, Doc)
        assert res.text == 'hello world'

    def test_list_to_doc(self):
        res = to_doc(['hello', 'world'])

        assert isinstance(res, Doc)
        assert res.text == 'hello world'

    def test_doc_to_doc(self):
        doc = Doc('hello world')
        res = to_doc(doc)

        assert isinstance(res, Doc)
        assert res is doc
        assert res.text == 'hello world'

    def test_unknown_to_doc(self):
        with self.assertRaises(ValueError):
            to_doc(1)


# class ViewTests(unittest.TestCase):
#     def setUp(self):
#         self.config = testing.setUp()
#
#     def tearDown(self):
#         testing.tearDown()
#
#     def test_my_view(self):
#         from .views import my_view
#         request = testing.DummyRequest()
#         info = my_view(request)
#         self.assertEqual(info['project'], 'EEA Corpus Server')
#
#
# class FunctionalTests(unittest.TestCase):
#     def setUp(self):
#         from eea_corpus import main
#         app = main({})
#         from webtest import TestApp
#         self.testapp = TestApp(app)
#
#     def test_root(self):
#         res = self.testapp.get('/', status=200)
#         self.assertTrue(b'Pyramid' in res.body)
