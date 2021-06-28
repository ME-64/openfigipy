import requests
import time
import urllib3
import json
import re
import os

import pandas as pd
import ratelimit
from cachetools import TTLCache, cached


class OpenFigiClient:

    BASE_URL = 'https://api.openfigi.com/v3'# {{{
    MAPPING_URL = BASE_URL + '/mapping'
    SEARCH_URL = BASE_URL + '/search'
    FILTER_URL = BASE_URL + '/filter'

    MAPPING_ENUM_URL = MAPPING_URL + '/values/{key}'

    ENUM_URL = 'https://www.openfigi.com/api/enumValues/v3'
    EXCH_CODE_URL = ENUM_URL + '/exchCode'
    SECURITY_TYPE_URL = ENUM_URL + '/securityType'
    MIC_CODE_URL = ENUM_URL + '/micCode'
    MARKET_SEC_DES_URL = ENUM_URL + '/marketSecDes'
    STATE_CODE_URL = ENUM_URL + '/stateCode'

    ALL_COLS = ['figi', 'name', 'exchCode', 'compositeFIGI',
            'securityType', 'marketSector', 'shareClassFIGI',
            'securityType2', 'securityDescription']

    # }}}

    def __init__(self, api_key=None, **kwargs):# {{{
        """
        Parameters
        ----------
        api_key : str or None
            The API key obtained from Open FIGI. This can also be specified with the
            environment variable OPENFIGI_API_KEY
        """

        self.api_key = api_key
        self.kwargs = kwargs 
        self._mapping_job_limit = 10
        self._search_filter_result_limit = 100
        # }}}

    def connect(self):# {{{
        """Start the API session with the API keys"""

        headers = {'Content-Type': 'Application/json'}

        if ('OPENFIGI_API_KEY' in os.environ.keys()) and (self.api_key is None):
            self.api_key = os.environ['OPENFIGI_API_KEY']


        if self.api_key is not None:
            headers.update({'X-OPENFIGI-APIKEY': self.api_key})
            self._mapping_job_limit = 25
            self._search_filter_result_limit = 150


        retries = urllib3.util.retry.Retry(total=5, 
                backoff_factor=2,
                status_forcelist=[429, 500, 503, 502, 413, 504])

        ada = requests.adapters.HTTPAdapter(max_retries=retries)
        self.session = requests.Session(**self.kwargs)
        self.session.mount('https://', ada)
        self.session.headers.update(headers)
        # assert_status_hook = lambda response, *args, **kwargs: response.raise_for_status()
        # self.session.hooks["response"] = [assert_status_hook]

        # }}}

    def disconnect(self):# {{{
        """Close the API session"""
        self.session.close()# }}}

    def _divide_chunks(self, l, n):# {{{
        # looping till length l
        for i in range(0, len(l), n): 
            yield l[i:i + n]# }}}

    @ratelimit.sleep_and_retry# {{{
    @ratelimit.limits(calls=25, period=9)
    def _send_auth_mapping_request(self, js):
        """send the complete request to the Open FIGI API, with API key rate limit

        Parameters
        ----------
        js: dict
            the data to be sent in the POST request
        """
        request = self.session.post(self.MAPPING_URL, json=js)
        return request.json()# }}}

    @ratelimit.sleep_and_retry# {{{
    @ratelimit.limits(calls=25, period=60)
    def _send_unauth_mapping_request(self, js):
        """send the complete request to the Open FIGI API, with non-API Key rate limit

        Parameters
        ----------
        json: dict
            the data to be sent in the POST request
        """
        request = self.session.post(self.MAPPING_URL, json=js)
        return request.json()# }}}

    def _send_mapping_request(self, js):# {{{
        """helper method to send requests with the correct rate limit
        Parameters
        ----------
        json: dict
            the data to be sent in the POST request
        """
        if self.api_key:
            json_result = self._send_auth_mapping_request(js)
        else:
            json_result = self._send_unauth_mapping_request(js)
        return json_result# }}}

    def _send_mapping_requests(self, jobs):# {{{
        """simple loop wrapper around `self._send_mapping_request`"""
        results = []
        for job in jobs:
            result = self._send_mapping_request(job)
            results.extend(result)
        return results# }}}

    def map_figis(self, figis):# {{{
        """Map a figi or iterable collection of figis to the Open FIGI database

        Parameters
        ----------
        figis: str or iterable
            The list of FIGIs (ID_BB_GLOBAL) to look-up using the API
        """
        if isinstance(figis, str):
            figis = [figis]

        df = pd.DataFrame({'idType': ['ID_BB_GLOBAL'] * len(figis), 'idValue': figis})

        return self.map(df)
        # }}}

    @ratelimit.sleep_and_retry# {{{
    @ratelimit.limits(calls=20, period=60)
    def _send_auth_search_filter_request(self, js, typ='search'):
        """send the complete request to the Open FIGI API, with API key rate limit

        Parameters
        ----------
        js: dict
            the data to be sent in the POST request
        """
        if typ == 'search':
            url = self.SEARCH_URL
        elif typ == 'filter':
            url = self.FILTER_URL

        request = self.session.post(url, json=js)
        return request.json()# }}}

    @ratelimit.sleep_and_retry# {{{
    @ratelimit.limits(calls=5, period=60)
    def _send_unauth_search_filter_request(self, js, typ='search'):
        """send the complete request to the Open FIGI API, with non-API Key rate limit

        Parameters
        ----------
        json: dict
            the data to be sent in the POST request
        """

        if typ == 'search':
            url = self.SEARCH_URL
        elif typ == 'filter':
            url = self.FILTER_URL
        request = self.session.post(url, json=js)

        return request.json()# }}}

    def _send_search_filter_request(self, js, typ='search'):# {{{
        """helper method to send requests with the correct rate limit
        Parameters
        ----------
        json: dict
            the data to be sent in the POST request
        """
        if self.api_key:
            json_result = self._send_auth_search_filter_request(js, typ=typ)
        else:
            json_result = self._send_unauth_search_filter_request(js, typ=typ)
        # print(js)
        return json_result# }}}

    @cached(cache=TTLCache(maxsize=10, ttl=43200))# {{{
    def get_mapping_enums(self, enum, use_cache=True):
        """get the list of valid values for a given key in the mapping query

        Parameters
        ----------
        enum: str 
            One of: idType, exchCode, micCode, currency, marketSecDes, securityType,
            securityType2, stateCode

        Returns
        -------
        results: list
            A list of the valid values for the given `enum` key
        """

        url = self.MAPPING_ENUM_URL.format(key=enum)
        request = self.session.get(url)
        results = request.json()
        return results['values']# }}}

    def _parse_mapping_result(self, results, df):# {{{
        """helper method to unnest the result of a mapping request and 
        turn into a dataframe

        Parameters
        ----------
        results: list
            The result of `OpenFigi._send_mapping_requests`

        df: pd.DataFrame
            The queried dataframe - used for linking a result back to an initial query
        """

        df.columns = ['q_' + x for x in df.columns.tolist()]
        df['query_number'] = range(df.shape[0])

        q_cols = df.columns.tolist()
        df_dict = df.to_dict('records')

        comb_cols = q_cols + self.ALL_COLS + ['result_number', 'status_code', 'status_message']

        data = pd.DataFrame(columns=comb_cols)

        cleaned_results = []

        for query, result in zip(df_dict, results):
            if 'data' in result.keys():
                for res_numb, inner_res in enumerate(result['data']):
                    tmp = query.copy()
                    tmp['status_code'] = 'success'
                    tmp['status_message'] = 'success'
                    tmp['result_number'] = res_numb
                    tmp.update(inner_res)
                    cleaned_results.append(tmp)
            elif 'warning' in result.keys():
                tmp = query.copy()
                tmp['status_code'] = 'warning'
                tmp['status_message'] = result['warning']
                tmp['result_number'] = 0
                cleaned_results.append(tmp)
            elif 'error' in result.keys():
                tmp = query.copy()
                tmp['status_code'] = 'error'
                tmp['status_message'] = result['error']
                tmp['result_number'] = 0
                cleaned_results.append(tmp)


        return pd.DataFrame(cleaned_results)# }}}

    def _clean_mapping_job_request(self, df_dict):# {{{
        """method to remove items where `None` is not a valid value
        to provide in the API. This will occur when you are making multiple mapping requests
        where they don't all use the same set of mapping keys"""

        valid_nones = ['strike', 'contractSize', 'coupon', 'expiration', 'maturity']

        new_df_dict = []
        for record in df_dict:
            new_record = record.copy()

            # filtering out the valid nones
            for k, v in record.items():
                if (k not in valid_nones) and pd.isnull(v):
                    new_record.pop(k)
            new_df_dict.append(new_record)

        return new_df_dict# }}}

    def map(self, df):# {{{
        """map a pandas DataFrame to values from the Open FIGI API

        Parameters
        ----------
        df: pd.DataFrame
            the dataframe to map, the columns should be valid parameters to be
            given to the Open FIGI API

        Returns
        -------
        result: pd.DataFrame
            returns the same dataframe as the initial input with the addition
            of the open figi result columns
        """

        assert 'idType' in df.columns
        assert 'idValue' in df.columns

        df = df.copy()

        df_dict = df.to_dict('records')

        df_dict = self._clean_mapping_job_request(df_dict)

        chunks = self._divide_chunks(df_dict, self._mapping_job_limit)

        result = self._send_mapping_requests(chunks)

        result_df = self._parse_mapping_result(result, df)
        return result_df# }}}

    def _build_search_filter_request(self, query=None, start=None, typ='search', **kwargs):# {{{
        """building a search or filter request"""

        data_dict = {}
        if typ == 'search':
            data_dict['query'] = query
        if start:
            data_dict['start'] = start

        if kwargs:
            data_dict.update(kwargs)
        return data_dict# }}}

    def _search_filter_pagnation(self, query='', typ='search', result_limit=100, **kwargs):# {{{
        js = self._build_search_filter_request(query=query, typ=typ, start=None, **kwargs)
        result = self._send_search_filter_request(js, typ=typ)

        tot = 0

        while 'data' in result and len(result['data']):
            for part in result['data']:
                yield part
                tot += 1
                if tot >= result_limit:
                    break
            if ('next' in result) and tot < result_limit:
                js = self._build_search_filter_request(query=query, typ=typ, start=result['next'], **kwargs)
                result = self._send_search_filter_request(js, typ=typ)
            else:
                break# }}}

    def search(self, query, result_limit=100, **kwargs):# {{{
        """Search the Open FIGI API for a given query

        Parameters
        ----------
        query: str
            The text term to search for
        result_limit: int
            The maximum number of results to return. This function will automatically
            handle the pagnation of requests
        kwargs
            Additional arguments provided to the Open FIGI API as part of the data object.
            Refer to the documentation for the list of possible values

        """

        typ = 'search'

        results = []

        gen_results = self._search_filter_pagnation(query=query, typ=typ, result_limit=result_limit, **kwargs)

        for i, result in enumerate(gen_results):
            results.append(result)
        return pd.DataFrame(results, columns=self.ALL_COLS)# }}}

    def filter(self, result_limit=100, **kwargs):# {{{
        """Filter the Open FIGI API for a given query

        Parameters
        ----------
        result_limit: int
            The maximum number of results to return. This function will automatically
            handle the pagnation of requests
        kwargs
            Additional arguments provided to the Open FIGI API as part of the data object.
            Refer to the documentation for the list of possible values

        """

        typ = 'filter'

        results = []


        gen_results = self._search_filter_pagnation(typ=typ, result_limit=result_limit, **kwargs)

        for i, result in enumerate(gen_results):
            results.append(result)
        return pd.DataFrame(results, columns=self.ALL_COLS)# }}}
