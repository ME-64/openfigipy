from openfigipy import OpenFigiClient
import pandas as pd
ofc = OpenFigiClient()
ofc.connect()

x = ofc.map_figis(['BBG0032FLQC3', 'BBG0032FLQC1', 'BBG00JPR0LW9'])


df = pd.DataFrame({'idType': ['ID_BB_GLOBAL', 'ID_BB_GLOBAL', 'ID_BB_GLOBAL'],
    'idValue': ['BBG0032FLQC3', 'BBG00JPR0LW9', 'BBG00JPR0LW6'], 'query_ref': ['x', 'y', 'z']})
x = ofc.map(df)

print(x)

x = ofc.search(';laksdfja;slkfjasd')

x = ofc.search('AAPL', result_limit=10, marketSecDes='Equity')

y = ofc.filter(exchCode='US', result_limit=101, marketSecDes='Equity')

