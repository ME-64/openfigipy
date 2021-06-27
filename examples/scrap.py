from openfigipy import OpenFigiClient


f = OpenFigiClient()
f.connect()

f = OpenFigiClient()
f.connect()

x = f.map_figis(['BBG0032FLQC3', 'BBG0032FLQC1', 'BBG00JPR0LW9'])


df = pd.DataFrame({'idType': ['ID_BB_GLOBAL', 'ID_BB_GLOBAL', 'ID_BB_GLOBAL'],
    'idValue': ['BBG0032FLQC3', 'BBG00JPR0LW9', 'BBG00JPR0LW6']})

df = pd.DataFrame({'idType': 'TICKER', 'idValue': 'IBM', 'marketSecDes': 'Equity',
    'currency': 'USD', 'exchCode': 'US'}, index=[0])

df = pd.DataFrame({'idType': ['TICKER', 'ID_BB_GLOBAL'],
    'idValue': ['IBM', 'BBG0032FLQC3'], 'currency': ['USD', 'USD'],
    'marketSecDes': ['Equity', 'Equity'], 'exchCode': ['US', None]})

x = f.map_dataframe(df)

print(x)
