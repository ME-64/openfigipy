from src.openfigipy import OpenFigiClient

import unittest
import logging as log

import pandas as pd


class OpenFigiTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # turn on logging to see behaviour of http client
        log.basicConfig(format='%(asctime)s %(levelname)s %(name)s : %(message)s', datefmt='%Y-%m-%d %I:%M:%S',
                        level=log.DEBUG)

    def setUp(self) -> None:
        self.ofc = OpenFigiClient()
        self.ofc.connect()

    def tearDown(self) -> None:
        self.ofc.disconnect()

    def test_map_single(self):
        df = pd.DataFrame({'idValue': ['BBG000BLNNH6'], 'idType': ['ID_BB_GLOBAL']})
        self._assert_figi_result(self.ofc.map(df))

    def test_map_figis_single(self):
        self._assert_figi_result(self.ofc.map_figis(['BBG000BLNNH6']))

    def test_search(self):  # {{{
        res = self.ofc.search('IBM', exchCode='US', marketSecDes='Equity', result_limit=1)
        self.assertEqual(res['securityType'].iloc[0], 'Common Stock')

    def _assert_figi_result(self, df: pd.DataFrame):
        self.assertEqual(df['shareClassFIGI'].iloc[0], 'BBG001S5S399')
        self.assertEqual(df['q_idType'].iloc[0], 'ID_BB_GLOBAL')
        self.assertEqual(df['q_idValue'].iloc[0], 'BBG000BLNNH6')
        self.assertEqual(df['securityType'].iloc[0], 'Common Stock')

    def test_map_batch(self):
        """
        Mapping API:
        Type of limitation	        Without API key	    With API key
        ------------------------------------------------------------
        Max amount of requests      25 per minute	    25 per 6 seconds
        Max jobs per request        10 jobs	            100 jobs
        """

        df: pd.DataFrame = pd.DataFrame({'idValue': isin[:60]})
        df['idType'] = 'ID_ISIN'
        df['marketSecDes'] = 'Equity'
        df['exchCode'] = 'US'
        df['currency'] = 'USD'

        req_resp: pd.DataFrame = self.ofc.map(df)
        self.assertEqual(len(req_resp), len(isin))
        req_resp.all(1)
        # crunch all return codes to their single unique values
        status_codes = list(set(req_resp['status_code'].to_list()))
        self.assertEqual(1, len(status_codes))
        self.assertEqual("success", status_codes[0])


if __name__ == '__main__':
    unittest.main()





isin = [
    'USB385641084', 'USB6S7WD1062', 'USC009481063', 'USF211071010', 'USG0R21B1045', 'USG004961029', 'USG007481066',
    'USG0083E1022', 'USG0084W1011', 'USG010461048', 'USG0112R1089', 'USG011251067', 'USG0120M1092', 'USG012021030',
    'USG0132V1055', 'USG0176J1090', 'USG017671052', 'USG0190X1003', 'USG0232J1019', 'USG0250X1075', 'USG026021034',
    'USG0360L1001', 'USG037AX1016', 'USG0370L1082', 'USG0370U1081', 'USG037091059', 'USG0371B1091', 'USG0403H1089',
    'USG0404A1028', 'USG041JN1062', 'USG0411R1061', 'USG041191069', 'USG0412A1028', 'USG044151086', 'USG0447J1028',
    'USG0450A1054', 'USG045531062', 'USG0457F1078', 'USG0464B1073', 'USG0477L1001', 'USG0509L1029', 'USG051551095',
    'USG0535E1067', 'USG0542N1075', 'USG054361039', 'USG0567U1019', 'USG0585R1061', 'USG0602B1001', 'USG0602B2090',
    'USG062421049', 'USG0625A1054', 'USG0633D1093', 'USG0633U1019', 'USG066071089', 'USG0682V1091', 'USG0684D1075',
    'USG0692U1090', 'USG0698L1038', 'USG070252014', 'USG072471026', 'USG0750C1083', 'USG0751N1030', 'USG0772R2088',
    'USG089081081', 'USG0904B1055', 'USG1R25Q1050', 'USG108301007', 'USG111961052', 'USG1125A1081', 'USG1144A1054',
    'USG1144D1093', 'USG1151C1011', 'USG115371001', 'USG1195N1057', 'USG1195R1061', 'USG1261Q1079', 'USG1329V1064',
    'USG1330M1039', 'USG1355V1038', 'USG144921057', 'USG1466B1038', 'USG1466R2079', 'USG148381092', 'USG1611B1073',
    'USG161691070', 'USG1686P1069', 'USG169621053', 'USG1739V1001', 'USG177661091', 'USG1890L1073', 'USG192761074',
    'USG195501055', 'USG1962Y1022', 'USG1992N1003', 'USG2R18K1054', 'USG2007L1055', 'USG2040C1048', 'USG2058L1038',
    'USG2072Q1043', 'USG210821058', 'USG2110U1090', 'USG2118P1024', 'USG2124G1043', 'USG2143T1037', 'USG215131099',
    'USG2161Y1094', 'USG216211007', 'USG2181K1054', 'USG218101099', 'USG2254A1090', 'USG2284B1010', 'USG2287A1000',
    'USG237261056', 'USG237731074', 'USG242371023', 'USG2425N1057', 'USG2426E1040', 'USG2445M1039', 'USG2519Y1085',
    'USG2540H1089', 'USG2554Y1040', 'USG257411029', 'USG258391048', 'USG270291002', 'USG2717B1082', 'USG273581037',
    'USG2758T1090', 'USG2770Y1022', 'USG2788T1037', 'USG279071074', 'USG283021008', 'USG283141053', 'USG283151029',
    'USG283651077', 'USG285531087', 'USG289231031', 'USG290181019', 'USG2911D1084', 'USG291831034', 'USG2952X1049',
    'USG2955B1091', 'USG3R19A1047', 'USG3R23A1082', 'USG3R2391010', 'USG3R33A1064', 'USG3R39W1021', 'USG300921032',
    'USG304011061', 'USG3075P1015', 'USG3104J1000', 'USG310671049', 'USG310701085', 'USG312491081', 'USG3141W1065',
    'USG3156P1033', 'USG316421043', 'USG316581002', 'USG316591084', 'USG3166T1037', 'USG3167F1024', 'USG3194F1096',
    'USG3195H1044', 'USG3198U1028', 'USG322191002', 'USG3223R1089', 'USG330321062', 'USG3312L1038', 'USG3323L1001',
    'USG332771074', 'USG338561081', 'USG341421026', 'USG3421J1063', 'USG350061085', 'USG359472028', 'USG364271050',
    'USG367381054', 'USG368161091', 'USG368261081', 'USG3710A1054', 'USG3728V1091', 'USG3728Y1031', 'USG372831010',
    'USG3770A1028', 'USG382451098', 'USG383271057', 'USG3855L1064', 'USG391081084', 'USG3922B1073', 'USG3932F1069',
    'USG3934J1063', 'USG3934P1024', 'USG3934V1091', 'USG393421031', 'USG394622082', 'USG396372058', 'USG3970D1049',
    'USG397141031', 'USG399731052', 'USG4000A1028', 'USG4022Y1040', 'USG4028H1052', 'USG4086B1073', 'USG4095T1072',
    'USG410891067', 'USG4204R1098', 'USG420411062', 'USG4388N1066', 'USG4411D1093', 'USG4412G1016', 'USG441251059'
]
