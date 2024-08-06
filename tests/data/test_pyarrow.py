import numpy as np
import pyarrow as pa

from fagfunksjoner.data.pyarrow import (
    cast_pyarrow_table_schema,
    restructur_pyarrow_schema,
)


def mock_starting_schema_to_cast():
    schema = pa.schema(
        [
            ("string1", pa.string()),
            ("string2", pa.string()),
            ("string3", pa.string()),
            ("int1", pa.int64()),
            ("int2", pa.int64()),
        ]
    )
    return schema


def mock_wanted_schema_after_cast():
    schema = pa.schema(
        [
            ("string1", pa.dictionary(pa.int8(), pa.string())),
            ("string2", pa.dictionary(pa.int32(), pa.string())),
            ("string3", pa.dictionary(pa.int32(), pa.string())),
            ("int1", pa.int64()),
            ("int2", pa.int32()),
        ]
    )
    return schema


def mock_data():
    size = 50
    data = pa.table(
        [
            pa.array(
                np.random.choice(["2023-01", "2023-02", "2023-03", "2023-04"], size)
            ),  # .dictionary_encode(),
            pa.array(
                np.random.choice(["xyz", "abc", "tre", "dfg"], size)
            ),  # .dictionary_encode(),
            pa.array(
                np.random.choice(["x1", "x2", "x3", "x4"], size)
            ),  # .dictionary_encode(),
            pa.array(np.random.randint(1000, 10000, size)),
            pa.array(np.random.randint(500, 5000, size)),
        ],
        names=["string1", "string2", "string3", "int1", "int2"],
    )
    schema = mock_starting_schema_to_cast()
    return data.cast(schema)


def test_cast_pyarrow_table_schema():
    testdata = mock_data()
    schema_start = mock_starting_schema_to_cast()
    schema_want = mock_wanted_schema_after_cast()
    assert testdata.schema.equals(schema_start)
    assert not testdata.schema.equals(schema_want)
    casted_data = cast_pyarrow_table_schema(testdata, schema_want)
    assert casted_data.schema.equals(schema_want)


def mock_starting_schema_to_reorder():
    schema = pa.schema(
        [
            ("string1", pa.string()),
            ("string2", pa.string()),
            ("string3", pa.string()),
            ("int1", pa.int64()),
            ("int2", pa.int64()),
        ]
    )
    return schema


def mock_wanted_schema_settings():
    schema = pa.schema(
        [
            ("string1", pa.dictionary(pa.uint8(), pa.string())),
            ("int1", pa.int64()),
            ("string2", pa.dictionary(pa.uint32(), pa.string())),
            ("int2", pa.int32()),
            ("string3", pa.dictionary(pa.uint32(), pa.string())),
        ]
    )
    return schema


def mock_wanted_schema_after_reorder():
    schema = pa.schema(
        [
            ("string1", pa.dictionary(pa.uint8(), pa.string())),
            ("string2", pa.dictionary(pa.uint32(), pa.string())),
            ("string3", pa.dictionary(pa.uint32(), pa.string())),
            ("int1", pa.int64()),
            ("int2", pa.int32()),
        ]
    )
    return schema


def test_restructur_pyarrow_schema():
    schema_want = mock_wanted_schema_after_reorder()
    schema_should = mock_wanted_schema_settings()
    schema_start = mock_starting_schema_to_reorder()
    schema = restructur_pyarrow_schema(schema_start, schema_should)
    assert schema_want.equals(schema)
