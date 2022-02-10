from src.openfigipy import OpenFigiClient
import requests
import unittest


class ClientTest(unittest.TestCase):

    def test_connect(self):
        ofp = OpenFigiClient()
        ofp.connect()
        self.assertTrue(hasattr(ofp, 'api_key'))
        self.assertTrue(hasattr(ofp, 'session'))
        self.assertTrue(isinstance(ofp.session, requests.Session))
        self.assertIn('Content-Type', ofp.session.headers.keys())

    def test_divide_chunks(self):
        long_li = list(range(100))
        short_li = [1, 2]

        ofp = OpenFigiClient()
        chunk_li = list(ofp._divide_chunks(long_li, 10))
        chunk_li_2 = list(ofp._divide_chunks(short_li, 10))

        self.assertEqual(len(chunk_li), 10)
        self.assertEqual(len(chunk_li_2), 1)
        self.assertEqual(len(chunk_li_2[0]), 2)
