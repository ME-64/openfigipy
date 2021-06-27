from openfigipy import OpenFigiClient
import requests

ofp = OpenFigiClient()

def test_connect():# {{{

    ofp.connect()
    assert hasattr(ofp, 'api_key')
    assert hasattr(ofp, 'session')

    assert isinstance(ofp.session, requests.Session)
    assert 'Content-Type' in ofp.session.headers.keys()# }}}


def test_build_mapping_request():# {{{

    data = {'idType': 'test', 'idValue': 'test1', 'strike': 'test2', 'coupon': 'test3'}

    job = ofp._build_mapping_request(idType='test', idValue='test1', strike='test2',
            coupon='test3', exchCode=None)

    assert data.items() == job.items()# }}}

def test_divide_chunks():

    long_li = list(range(100))
    short_li = [1, 2]

    chunk_li = ofp._divide_chunks(long_li, 10)
    chunk_li_2 = ofp.divide_chunks(short_li, 10)

    assert len(chunk_li) == 10
    assert len(chunk_li_2) == 1
    assert len(chunk_li_2[0]) == 2





