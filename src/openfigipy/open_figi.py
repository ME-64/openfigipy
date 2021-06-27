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

    MAPPING_ENUM_URL = MAPPING_URL + '/values/{key}'

    ENUM_URL = 'https://www.openfigi.com/api/enumValues/v3'
    EXCH_CODE_URL = ENUM_URL + '/exchCode'
    SECURITY_TYPE_URL = ENUM_URL + '/securityType'
    MIC_CODE_URL = ENUM_URL + '/micCode'
    MARKET_SEC_DES_URL = ENUM_URL + '/marketSecDes'
    STATE_CODE_URL = ENUM_URL + '/stateCode'# }}}

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
        # }}}

    def connect(self):# {{{
        """Start the API session with the API keys"""

        headers = {'Content-Type': 'Application/json'}

        if ('OPENFIGI_API_KEY' in os.environ.keys()) and (self.api_key is None):
            self.api_key = os.environ['OPENFIGI_API_KEY']


        if self.api_key is not None:
            headers.update({'X-OPENFIGI-APIKEY': self.api_key})
            self._mapping_job_limit = 25


        retries = urllib3.util.retry.Retry(total=2, 
                backoff_factor=1,
                status_forcelist=[429, 500, 503, 502, 413, 504])

        ada = requests.adapters.HTTPAdapter(max_retries=retries)
        self.session = requests.Session(**self.kwargs)
        self.session.mount('https://', ada)
        self.session.headers.update(headers)
        assert_status_hook = lambda response, *args, **kwargs: response.raise_for_status()
        self.session.hooks["response"] = [assert_status_hook]

        # }}}

    def disconnect(self):# {{{
        """Close the API session"""
        self.session.close()# }}}

    def _build_mapping_request(self, **kwargs):# {{{
        """Build the inner jobs for a mapping request

        Parameters
        ----------
        **kwargs
            A list of keywords that the Open FIGI API Mapping request takes
        """

        job = {}
        for k, v in kwargs.items():
            if (k not in ['strike', 'contractSize', 'coupon', 'expiration', 'maturity']) \
                    and pd.isnull(v):
                        continue
            job.update(kwargs)
        print(job)
        return job# }}}

    def _build_figi_request(self, figis):# {{{
        """build a requeset that specifically looks up using ID_BB_GLOBAL

        Parameters
        ----------
        figis: str or iterable
            The list of FIGIs (ID_BB_GLOBAL) to look-up using the API

        """

        if isinstance(figis, str):
            figis = [figis]

        maps = []

        for fig in figis:
            tmp = self._build_mapping_request(idType='ID_BB_GLOBAL', idValue=fig, includeUnlistedEquities=True)
            maps.append(tmp)

        maps = list(self._divide_chunks(maps, self._mapping_job_limit))
        return maps# }}}

    def _divide_chunks(self, l, n):# {{{
        # looping till length l
        for i in range(0, len(l), n): 
            yield l[i:i + n]# }}}

    @ratelimit.sleep_and_retry# {{{
    @ratelimit.limits(calls=25, period=6)
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

    def _infer_not_found_figi(self, lookup_figis, found_figis):# {{{
        """workout the figis where no result was found when querying the API

        Parameters
        ----------
        lookup_figis: list
            list of the FIGIs that were queried
        found_figis: list
            list of the FIGIs that were returned from the query

        """

        base_cols = ['ticker', 'exchCode', 'compositeFIGI', 'securityType', 'marketSector',
                'shareClassFIGI', 'securityType2', 'securityDescription']


        if len(found_figis) == 0:
            ndf = pd.Series(lookup_figis).to_frame(name='figi')
            for col in base_cols:
                ndf[col] = None
            return ndf

        lookup_series = pd.Series(lookup_figis)
        found_series = pd.Series(found_figis)
        not_found_series = lookup_series.loc[~lookup_series.isin(found_series)]

        ndf = not_found_series.to_frame(name='figi')
        for col in base_cols:
            ndf[col] = None
        ndf['found_flag'] = False
        return ndf# }}}

    def map_figis(self, figis):# {{{
        """Map a figi or iterable collection of figis to the Open FIGI database

        Parameters
        ----------
        figis: str or iterable
            The list of FIGIs (ID_BB_GLOBAL) to look-up using the API
        """
        if isinstance(figis, str):
            figis = [figis]

        # self._validate_figis(figis)


        df = pd.DataFrame({'idType': ['ID_BB_GLOBAL'] * len(figis), 'idValue': figis})

        return self.map_dataframe(df)
        # }}}

    def _validate_figis(self, figis):# {{{
        """perform checks to ensure valid figis are supplied"""

        if isinstance(figis, str):
            figis = [figis]

        for figi in figis:
            assert figi.upper()[2] == 'G'
            assert len(figi) == 12
            mid_figi = figi[3:10]
            assert not any(x in mid_figi.lower() for x in ['a', 'e', 'i', 'o', 'u'])
        return# }}}

    @ratelimit.sleep_and_retry# {{{
    @ratelimit.limits(calls=20, period=60)
    def _send_auth_search_request(self, js):
        """send the complete request to the Open FIGI API, with API key rate limit

        Parameters
        ----------
        js: dict
            the data to be sent in the POST request
        """
        request = self.session.post(self.SEARCH_URL, json=js)
        return request.json()# }}}

    @ratelimit.sleep_and_retry# {{{
    @ratelimit.limits(calls=5, period=60)
    def _send_unauth_search_request(self, js):
        """send the complete request to the Open FIGI API, with non-API Key rate limit

        Parameters
        ----------
        json: dict
            the data to be sent in the POST request
        """
        request = self.session.post(self.SEARCH_URL, json=js)
        return request.json()# }}}

    def _send_search_request(self, js):# {{{
        """helper method to send requests with the correct rate limit
        Parameters
        ----------
        json: dict
            the data to be sent in the POST request
        """
        if self.api_key:
            json_result = self._send_auth_search_request(js)
        else:
            json_result = self._send_unauth_search_request(js)
        return json_result# }}}

    def _send_search_requests(self, jobs):# {{{
        """simple loop wrapper around `self._send_search_request`"""
        results = []
        for job in jobs:
            result = self._send_search_request(job)
            results.extend(result)
        return results# }}}

    @cached(cache=TTLCache(maxsize=10, ttl=43200))# {{{
    def _get_mapping_enums(self, enum, use_cache=True):
        """get the list of valid values for a given key in the mapping query

        Parameters
        ----------
        enum: str 
            One of: idType, exchCode, micCode, currency, marketSecDes, securityType,
            securityType2, stateCode
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


        data = pd.DataFrame(columns=q_cols + ['result_number', 'figi', 'name', 'ticker', 'exchCode', 
            'compositeFIGI', 'securityType', 'marketSector', 'shareClassFIGI',
            'securityType2', 'securityDescription', 'status_code', 'status_message'])

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

    def map_dataframe(self, df):# {{{
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

