import datetime
import unittest
from pathlib import Path

from common.utils import dateCETstr_to_tzdt, list_files


class UtilsTest(unittest.TestCase):

    def test__dades_test_environment(self):
        pass

    def test__datestr_to_tzdt(self):
        time = dateCETstr_to_tzdt("20210301")
        expected_time = datetime.datetime(
            2021, 2, 28, 23, 0, tzinfo=datetime.timezone.utc
        )
        self.assertEqual(time, expected_time)

    def test__list_files(self):
        directory = "testdata/MARGINALPDBC/"
        files = list_files(directory)

        expected_files = [
            Path("testdata/MARGINALPDBC/marginalpdbc_20220103.1"),
            Path("testdata/MARGINALPDBC/marginalpdbc_20211213.1"),
            Path("testdata/MARGINALPDBC/marginalpdbc_20211213.1_to_shape"),
        ]

        expected_files.sort()
        files.sort()
        assert set(expected_files).issubset(set(files))
