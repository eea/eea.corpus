from eea.corpus.utils import to_doc
from eea.corpus.utils import to_text
from unittest.mock import Mock, patch
from textacy.doc import Doc
import unittest


class TestMiscUtils(unittest.TestCase):
    """ Tests for misc utils
    """

    def test_rand(self):
        from eea.corpus.utils import rand
        x = rand(10)
        assert len(x) == 10
        assert x.isalnum()

    def test_hashed_id(self):
        from eea.corpus.utils import hashed_id
        assert hashed_id({}) == \
            "d14a028c2a3a2bc9476102bb288234c415a2b01f828ea62ac5b3e42f"
        assert hashed_id({'a': 'b'}) == \
            "abd37534c7d9a2efb9465de931cd7055ffdb8879563ae98078d6d6d5"

    def test_invalid_document_name(self):
        from eea.corpus.utils import document_name
        req = Mock()

        req.matchdict = {}
        with self.assertRaises(ValueError):
            # this is not a valid document name
            document_name(req)

        req.matchdict = {'doc': 'first'}
        with self.assertRaises(ValueError):
            # this is not a valid document name
            document_name(req)

    @patch('eea.corpus.utils.is_valid_document')
    def test_valid_document_name(self, is_valid_document):
        from eea.corpus.utils import document_name
        req = Mock()
        req.matchdict = {'doc': 'first'}
        is_valid_document.return_value = True
        assert document_name(req) == 'first'


class TestConvertorDecorators(unittest.TestCase):
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

    def test_str_to_text(self):
        res = to_text('hello world')
        assert res == 'hello world'

    def test_list_to_text(self):
        res = to_text(['hello', 'world'])
        assert res == 'hello world'

    def test_doc_to_text(self):
        res = to_text(Doc('hello world'))
        assert res == 'hello world'

    def test_unknown_to_text(self):
        with self.assertRaises(ValueError):
            to_text(1)


# from pyramid import testing
# import unittest
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
# def test_doctest():
#     finder = doctest.DocTestFinder(exclude_empty=False)
#     suite = doctest.DocTestSuite(test_finder=finder)
#     suite.run()
#
# def load_tests(loader, tests, ignore):
#     from eea.corpus import utils
#     tests.addTests(doctest.DocTestSuite(utils))
