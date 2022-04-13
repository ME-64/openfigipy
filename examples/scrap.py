from openfigipy import OpenFigiClient
import pandas as pd
ofc = OpenFigiClient()
ofc.connect()

x = ofc.map_figis(['BBG0032FLQC3', 'BBG0032FLQC1', 'BBG00JPR0LW9'])


df = pd.DataFrame({'idType': ['ID_BB_GLOBAL', 'ID_BB_GLOBAL', 'ID_BB_GLOBAL'],
    'idValue': ['BBG0032FLQC3', 'BBG00JPR0LW9', 'BBG00JPR0LW6'], 'query_ref': ['x', 'y', 'z']})
y = df.to_dict('records')
ofc._send_auth_mapping_request(y, True)

df = pd.DataFrame({'idType': 'TICKER', 'idValue': 'IBM', 'marketSecDes': 'Equity',
    'currency': 'USD', 'exchCode': 'US'}, index=[0])

df = pd.DataFrame({'idType': ['TICKER', 'ID_BB_GLOBAL'],
    'idValue': ['IBM', 'BBG0032FLQC3'], 'currency': ['USD', 'USD'],
    'marketSecDes': ['Equity', 'Equity']})

x = ofc.map(df)

print(x)

x = ofc.search(';laksdfja;slkfjasd')

x = ofc.search('AAPL', result_limit=10, marketSecDes='Equity')

y = ofc.filter(exchCode='US', result_limit=101, marketSecDes='Equity')


q = ofc._build_search_filter_request(typ='filter', exchCode='US')

res = ofc._send_search_filter_request(q, typ='filter')
