import pytest
from unittest.mock import patch, MagicMock
from fagfunksjoner.prodsone.oradb import Oracle  # Replace 'fagfunksjoner.prodsone.oradb' with the actual module name

# Sample data for testing
sample_sql = "SELECT * FROM sample_table"
sample_update_sql = "UPDATE sample_table SET col1 = :1 WHERE col2 = :2"
sample_data = [("value1", "value2"), ("value3", "value4")]
sample_batchsize = 2
sample_result = [{"col1": "value1", "col2": "value2"}, {"col1": "value3", "col2": "value4"}]


@pytest.fixture
def oracle_instance():
    with patch("fagfunksjoner.prodsone.oradb.getuser", return_value="test_user"):
        with patch("fagfunksjoner.prodsone.oradb.getpass", return_value="test_pw"):
            return Oracle("test_db", "test_pw")


@patch("oracledb.connect")
def test_select(mock_connect, oracle_instance):
    mock_cursor = MagicMock()
    mock_cursor.description = [("COL1",), ("COL2",)]
    mock_cursor.fetchall.return_value = [("value1", "value2"), ("value3", "value4")]
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_connect.return_value.__enter__.return_value = mock_conn

    result = oracle_instance.select(sample_sql)

    assert result == sample_result
    mock_cursor.execute.assert_called_once_with(sample_sql)


@patch("oracledb.connect")
def test_update_or_insert(mock_connect, oracle_instance):
    mock_cursor = MagicMock()
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_connect.return_value.__enter__.return_value = mock_conn

    oracle_instance.update_or_insert(sample_update_sql, sample_data)

    mock_cursor.executemany.assert_called_once_with(sample_update_sql, sample_data)
    mock_conn.commit.assert_called_once()


@patch("oracledb.connect")
def test_select_many(mock_connect, oracle_instance):
    mock_cursor = MagicMock()
    mock_cursor.description = [("COL1",), ("COL2",)]
    mock_cursor.fetchmany.side_effect = [
        [("value1", "value2"), ("value3", "value4")], []
    ]
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_connect.return_value.__enter__.return_value = mock_conn

    result = oracle_instance.select_many(sample_sql, sample_batchsize)

    assert result == sample_result
    mock_cursor.execute.assert_called_once_with(sample_sql)
    mock_cursor.fetchmany.assert_called_with(sample_batchsize)


def test_passw(oracle_instance):
    with patch("fagfunksjoner.prodsone.oradb.getpass", return_value="new_pw"):
        oracle_instance._passw(None)
        assert oracle_instance.pw == "new_pw"

    oracle_instance._passw("direct_pw")
    assert oracle_instance.pw == "direct_pw"


@patch("oracledb.connect")
def test_context_manager(mock_connect, oracle_instance):
    mock_cursor = MagicMock()
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_connect.return_value = mock_conn

    with oracle_instance as cursor:
        print("Entering context manager")
        cursor.execute("SELECT 1")
        print(f"Cursor execute called on: {cursor}")
        print("Executed query")

    print(f"Mock cursor execute call count: {mock_cursor.execute.call_count}")
    mock_cursor.execute.assert_called_once_with("SELECT 1")
    mock_cursor.__exit__.assert_called()
    mock_conn.__exit__.assert_called()
    assert not hasattr(oracle_instance, "cur")
    assert not hasattr(oracle_instance, "conn")


def test_close(oracle_instance):
    oracle_instance.close()
    assert not hasattr(oracle_instance, "user")
    assert not hasattr(oracle_instance, "pw")
    assert not hasattr(oracle_instance, "db")