from datetime import datetime, date
import pytest
from fagfunksjoner.dapla.standards.time import *

@pytest.mark.parametrize("date_input, exprected_string",
                         [(datetime(2016, 1, 1, 12, 1, 1), "2016-01-01T12:01:01"),
                          (datetime(2022, 6, 1, 12, 1, 1), "2022-06-01T12:01:01")])
def test_date_time(date_input, exprected_string):
    date = date_time(date_input)
    assert date == exprected_string


@pytest.mark.parametrize("date_input, exprected_string",
                         [(date(2016, 1, 1), "2016-01-01"),
                          (date(2022, 6, 1), "2022-06-01")])
def test_date(date_input, exprected_string):
    date = date(date_input)
    assert date == exprected_string


@pytest.mark.parametrize("date_input, exprected_string",
                         [(date(2016, 1, 1), "2016-01"),
                          (date(2022, 6, 1), "2022-06")])
def test_month(date_input, exprected_string):
    date = month(date_input)
    assert date == exprected_string


@pytest.mark.parametrize("date_input, exprected_string",
                         [(date(2016, 1, 1), "2016"),
                          (date(2022, 6, 1), "2022")])
def test_year(date_input, exprected_string):
    date = year(date_input)
    assert date == exprected_string


@pytest.mark.parametrize("date_input, exprected_string",
                         [(date(2016, 1, 1), "2015-53"),
                          (date(2022, 6, 1), "2022-22")])
def test_week(date_input, exprected_string):
    date = week(date_input)
    assert date == exprected_string


@pytest.mark.parametrize("date_input, exprected_string",
                         [(date(2016, 1, 1), "2016-001"),
                          (date(2022, 6, 1), "2022-152")])
def test_year_days(date_input, exprected_string):
    date = year_days(date_input)
    assert date == exprected_string


@pytest.mark.parametrize("date_input, exprected_string",
                         [(date(2016, 1, 1), "2016-Q1"),
                          (date(2022, 6, 1), "2022-Q2")])
def test_quarterly(date_input, exprected_string):
    date = quarterly(date_input)
    assert date == exprected_string


@pytest.mark.parametrize("date_input, exprected_string",
                         [(date(2016, 1, 1), "2016-B1"),
                          (date(2022, 6, 1), "2022-B3")])
def test_bimester(date_input, exprected_string):
    date = bimester(date_input)
    assert date == exprected_string


@pytest.mark.parametrize("date_input, exprected_string",
                         [(date(2016, 1, 1), "2016-T1"),
                          (date(2022, 6, 1), "2022-T2")])
def test_triannual(date_input, exprected_string):
    date = triannual(date_input)
    assert date == exprected_string


@pytest.mark.parametrize("date_input, exprected_string",
                         [(date(2016, 1, 1), "2016-H1"),
                          (date(2022, 6, 1), "2022-H1")])
def test_halfyear(date_input, exprected_string):
    date = halfyear(date_input)
    assert date == exprected_string