import requests
import pandas as pd
import time
import ratelimit
import urllib3
import json

_API_KEY = '36256111-b28e-45e1-abcf-6db9960da7db'

class OpenFigi:

    BASE_URL = 'https://api.openfigi.com'
    MAPPING_URL = BASE_URL + '/v3/mapping'
    SEARCH_URL = BASE_URL + '/v3/search'

    def __init__(self, api_key=None, **kwargs):# {{{
        """
        Parameters
        ----------
        api_key : str
            The API key obtained from Open FIGI
        """

        self.api_key = api_key
        self.kwargs = kwargs 
        self._mapping_job_limit = 10
        # }}}

    def connect(self):# {{{
        """Start the API session with the API keys"""

        headers = {'Content-Type': 'Application/json'}
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
        job.update(kwargs)
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
        print(json_result)
        return json_result# }}}

    def _send_mapping_requests(self, jobs):# {{{
        """simple loop wrapper around `self._send_mapping_request`"""
        results = []
        for job in jobs:
            result = self._send_mapping_request(chunk)
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

        map_jobs = self._build_figi_request(figis) # creates the list of jobs to send to API

        results = self._send_mapping_request(map_jobs) # sends the list of jobs to the API

        final_result = []
        # iterate through and filter out those that results weren't found for
        for res in results:
            if 'data' in res.keys():
                if len(res['data']) > 1:
                    raise ValueError(f'multiple results found, not expected for figi search {res}')
                final_result.append(res['data'][0])

        found_figis = pd.DataFrame(final_result)
        found_figis['found_flag'] = True

        not_found_figis = self._infer_not_found_figi(figis, found_figis['figi'].tolist())

        combined_df = combined_df.append(not_found_figis, ignore_index=True)
        combined_df.drop_duplicates(inplace=True)

        return combined_df# }}}


f = OpenFigi(api_key=_API_KEY)
f.connect()

x = f.map_figis('BBG0032FLQC3')
