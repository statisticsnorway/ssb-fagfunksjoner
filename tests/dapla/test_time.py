import datetime as dt
import pytest
from fagfunksjoner.dapla.standards.time import date_time, date, month, year, week, year_days, quarterly, bimester, triannual, halfyear

@pytest.mark.parametrize("date_input,exprected_string",
                         [(dt.datetime(2016, 1, 1, 12, 1, 1), "2016-01-01T12:01:01"),
                          (dt.datetime(2022, 6, 1, 12, 1, 1), "2022-06-01T12:01:01")])
def test_date_time(date_input, exprected_string):
    dato = date_time(date_input)
    assert dato == exprected_string


@pytest.mark.parametrize("date_input,exprected_string",
                         [(dt.date(2016, 1, 1), "2016-01-01"),
                          (dt.date(2022, 6, 1), "2022-06-01")])
def test_date(date_input, exprected_string):
    dato = date(date_input)
    assert dato == exprected_string


@pytest.mark.parametrize("date_input,exprected_string",
                         [(dt.date(2016, 1, 1), "2016-01"),
                          (dt.date(2022, 6, 1), "2022-06")])
def test_month(date_input, exprected_string):
    dato = month(date_input)
    assert dato == exprected_string


@pytest.mark.parametrize("date_input,exprected_string",
                         [(dt.date(2016, 1, 1), "2016"),
                          (dt.date(2022, 6, 1), "2022")])
def test_year(date_input, exprected_string):
    dato = year(date_input)
    assert dato == exprected_string


@pytest.mark.parametrize("date_input,exprected_string",
                         [(dt.date(2016, 1, 1), "2015W53"),
                          (dt.date(2022, 6, 1), "2022W22")])
def test_week(date_input, exprected_string):
    dato = week(date_input)
    assert dato == exprected_string


@pytest.mark.parametrize("date_input,exprected_string",
                         [(dt.date(2016, 1, 1), "2016-001"),
                          (dt.date(2022, 6, 1), "2022-152")])
def test_year_days(date_input, exprected_string):
    dato = year_days(date_input)
    assert dato == exprected_string


@pytest.mark.parametrize("date_input,exprected_string",
                         [(dt.date(2016, 1, 1), "2016-Q1"),
                          (dt.date(2022, 6, 1), "2022-Q2")])
def test_quarterly(date_input, exprected_string):
    dato = quarterly(date_input)
    assert dato == exprected_string


@pytest.mark.parametrize("date_input,exprected_string",
                         [(dt.date(2016, 1, 1), "2016-B1"),
                          (dt.date(2022, 6, 1), "2022-B3")])
def test_bimester(date_input, exprected_string):
    dato = bimester(date_input)
    assert dato == exprected_string


@pytest.mark.parametrize("date_input,exprected_string",
                         [(dt.date(2016, 1, 1), "2016-T1"),
                          (dt.date(2022, 6, 1), "2022-T2")])
def test_triannual(date_input, exprected_string):
    dato = triannual(date_input)
    assert dato == exprected_string


@pytest.mark.parametrize("date_input,exprected_string",
                         [(dt.date(2016, 1, 1), "2016-H1"),
                          (dt.date(2022, 6, 1), "2022-H1")])
def test_halfyear(date_input, exprected_string):
    dato = halfyear(date_input)
    assert dato == exprected_string