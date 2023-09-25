from fagfunksjoner.paths import get_latest_fileversions
import pytest


def test_get_latest_fileversions():
    """Tests the get_latest_fileversions function"""
    testfiles = ['gs://dir/data1_v1.parquet', 'gs://dir/data1_v2.parquet',
                 'gs://dir/data2_v1.parquet', 'gs://dir/data2_v2.parquet',
                 'gs://dir/data2_v3.parquet', 'gs://dir/data3_v1.parquet']
    unique_filenames = sorted(list(set([file.split('_v')[0] for file in testfiles])))
    latest_fileversions = get_latest_fileversions(testfiles)
    assert len(unique_filenames) == len(latest_fileversions)
    for f_name, f_latest_v in zip(unique_filenames, latest_fileversions):
        assert f_latest_v.startswith(f_name)