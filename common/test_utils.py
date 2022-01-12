import unittest

import datetime
from pathlib import Path

from common.utils import(
    list_files,
    dateCETstr_to_tzdt
)

class UtilsTest(unittest.TestCase):

    def test__dades_test_environment(self):
        pass

    def test__datestr_to_tzdt(self):
        filename = 'deadbeef.csv'
        time = dateCETstr_to_tzdt('20210301')
        expected_time = datetime.datetime(2021,2,28,23,0, tzinfo=datetime.timezone.utc)
        self.assertEqual(time, expected_time)

    def test__list_files(self):
        directory = 'testdata/MARGINALPDBC/'
        files = list_files(directory)

        expected_files = [Path('testdata/MARGINALPDBC/marginalpdbc_20220103.1'),Path('testdata/MARGINALPDBC/marginalpdbc_20211213.1')]
        
        expected_files.sort()
        files.sort()
        self.assertListEqual(files, expected_files)