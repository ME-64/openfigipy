# openfigipy

> A python wrapper around the [Open FIGI v3 API](https://www.openfigi.com/) that leverages
> pandas DataFrames to make requests and return data.


Features Include
-----------------

- Automatically throttles requests to respect the API rate limit
- Automatically handles the chunking and retrieval of mapping jobs
- Automatically handles the pagination of search requests
- Queries are given by providing the relevant method with a pandas DataFrame,
  allowing easy integration with existing reference data pipelines.

Getting Started
---------------

```python3

import pandas as pd
from openfigipy import OpenFigiClient

# api key can either be given with the api_key argument
# or set as the environment variable `OPENFIGI_API_KEY
ofc = OpenFigiClient()

# establish a requests session
ofc.connect()

# create a dataframe of look-ups - each row represents one query that will
# be batched in jobs
# the column headers represent the relevant key from the open figi api
df = pd.DataFrame({'idType': ['TICKER', 'ID_BB_GLOBAL'],
    'idValue': ['IBM', 'BBG0032FLQC3'], 'currency': ['USD', 'USD'],
    'marketSecDes': ['Equity', 'Equity'], 'exchCode': ['US', None]})

print(df)

#          idType       idValue currency marketSecDes
# 0        TICKER           IBM      USD       Equity
# 1  ID_BB_GLOBAL  BBG0032FLQC3      USD       Equity


result = ofc.map_dataframe(df)

print(result.head())

#        q_idType     q_idValue q_currency  ... shareClassFIGI securityType2  securityDescription
# 0        TICKER           IBM        USD  ...   BBG001S5S399  Common Stock                  IBM
# 1  ID_BB_GLOBAL  BBG0032FLQC3        USD  ...   BBG001S5N8V8  Common Stock                 AAPL

print(result.columns.tolist())
# ['q_idType',
#  'q_idValue',
#  'q_currency',
#  'q_marketSecDes',
#  'q_exchCode',
#  'query_number',
#  'status_code',
#  'status_message',
#  'figi',
#  'name',
#  'ticker',
#  'exchCode',
#  'compositeFIGI',
#  'securityType',
#  'marketSector',
#  'shareClassFIGI',
#  'securityType2',
#  'securityDescription']
```

The resulting dataframe will keep your original query columns, prefixed with
`q_` as well as the documented response from the Open FIGI API. This is to
ensure there isn't an overlap i.e. if your query contains `exchCode` and the
results do to. There are also some additional helper columns described below
too.

`query_number`: Shows which query the result is related to, can be helpful when
a query returns multiple matches.

`result_number`: Shows the order in which the results were returned by the Open
FIGI API. Generally the best match is shown first (i.e. `result_number` 0)


`status_code`: one of ('success', 'warning', 'error') as per the documentation
on the Open FIGI API.

`status_message`: The associated message with the given `status_code`. Helpful
for understanding why results might not have been returned.


Running tests
-------------

To run all unit tests for this module:
```shell
$ python -m unittest discover
```
Please be aware some tests might take some time since they call external APIs.

Todo
----

- Setup testing (integration and unit testing). Learn about mocking
- Setup automatic documentation generation w/ Sphinx
- explore if type hinting could help
- Setup automatic linting and checking
- Setup continuous integration
    - tests
    - documentation
    - pypi publishing

