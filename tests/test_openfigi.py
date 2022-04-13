from src.openfigipy import OpenFigiClient
import requests

from openfigipy import OpenFigiClient

import requests
import pandas as pd

def test_connect():# {{{
    ofc = OpenFigiClient()
    ofc.connect()
    assert hasattr(ofc, 'api_key')
    assert hasattr(ofc, 'session')
    assert isinstance(ofc.session, requests.Session)
    assert 'Content-Type' in ofc.session.headers.keys()
    ofc.disconnect()# }}}

ofc = OpenFigiClient()
ofc.connect()


def test_map():# {{{

    df = pd.DataFrame({'idValue': ['BBG000BLNNH6'], 'idType': ['ID_BB_GLOBAL']})

    res = ofc.map(df)
    assert res['shareClassFIGI'].iloc[0] == 'BBG001S5S399'
    assert res['q_idType'].iloc[0] == 'ID_BB_GLOBAL'
    assert res['q_idValue'].iloc[0] == 'BBG000BLNNH6'
    assert res['securityType'].iloc[0] == 'Common Stock'


    res = ofc.map_figis(['BBG000BLNNH6'])
    assert res['shareClassFIGI'].iloc[0] == 'BBG001S5S399'
    assert res['q_idType'].iloc[0] == 'ID_BB_GLOBAL'
    assert res['q_idValue'].iloc[0] == 'BBG000BLNNH6'
    assert res['securityType'].iloc[0] == 'Common Stock'# }}}

def test_search():# {{{

    res = ofc.search('IBM', exchCode='US', marketSecDes='Equity', result_limit=1)
    assert res['securityType'].iloc[0] == 'Common Stock'
    assert res['securityType'].iloc[0] == 'Common Stock'# }}}
