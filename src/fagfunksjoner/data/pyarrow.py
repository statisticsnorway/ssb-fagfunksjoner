import pyarrow as pa


def cast_pyarrow_table_schema(data: pa.Table, schema: pa.Schema) -> pa.Table:
    """Set correct schema on Pyarrow Table, especially when dictionary datatype is wanted.

    Args:
        data: The pyarrow table data
        schema: The wanted schema to cast to the table data.
            All columns in pyarrow table must be present in the schema.
            The order of the columns in the schema will be used.

    Returns:
        pa.Table: A new pyarrow table with correct schema.
    """
    # Pyarrow has only partial type hints. All type: ignore in this function has been
    # checked with the pyarrow documentation, and the implementation is correct.
    newdata = []
    newnames = []
    for field in schema:
        if type(field.type) is pa.DictionaryType:  # type: ignore
            new = data.column(field.name).dictionary_encode().cast(field.type)  # type: ignore
        else:
            new = data.column(field.name).cast(field.type)  # type: ignore
        newdata.append(new)
        newnames.append(field.name)
    return pa.table(newdata, names=newnames)  # type: ignore


def restructur_pyarrow_schema(
    inuse_schema: pa.Schema, wanted_schema: pa.Schema
) -> pa.Schema:
    """Reorder and set the schema you want to fit the in-use schema.

    The column names in the in use schema must be present in the wanted schema.
    They should preferably have the same datatype, but not necessarily
    the same datatype settings, especially when it comes to DictionaryType.
    If datatypes are different, the wanted schema is used. And if DictionaryType
    is present in that case, you must then change your datatypes before casting
    this new schema.

    Args:
        inuse_schema: The schema that is in use of your pyarrow dataset or table.
        wanted_schema: The schema that you want, but it is not in the same order of
            the schema that is in use.

    Returns:
        pa.Schema: A new pyarrow schema that has the same order as the in use schema,
            but with the correct datatypes from the schema that we want.
    """
    # Pyarrow has only partial type hints. All type: ignore in this function has been
    # checked with the pyarrow documentation, and the implementation is correct.
    for col in inuse_schema.names:
        assert col in wanted_schema.names
    newfields = []
    for name in inuse_schema.names:
        inuse_field_index = inuse_schema.get_field_index(name)
        wanted_field_index = wanted_schema.get_field_index(name)
        inuse_field = inuse_schema.field(inuse_field_index)  # type: ignore
        wanted_field = wanted_schema.field(wanted_field_index)  # type: ignore

        if type(inuse_field.type) is type(wanted_field.type):  # type: ignore
            newfields.append(inuse_field.with_type(wanted_field.type))  # type: ignore
        else:
            newfields.append(wanted_field)
    return pa.schema(newfields)
