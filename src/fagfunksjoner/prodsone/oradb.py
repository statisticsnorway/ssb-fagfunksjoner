from getpass import getpass, getuser
from types import TracebackType
from typing import Any, cast

import oracledb


class Oracle:
    """Class for working with an Oracle database with most common queries.

    This class supports the most used SQL queries to a table in a database.
    It gives us the possibilities to query multiple times since the class
    has the credentials needed stored until the user closes the
    connection.

    Note:
        User must remember to call the close method after final use.

    Attributes:
        user (str): user id
        db (str): database name
        pw (str): user password
        conn (oracledb.Connection): database connection if using context manager
        cur (oracledb.Cursor): database cursor if using context manager
    """

    def __init__(self, db: str, pw: str | None = None) -> None:
        """The instantiation of the class.

        Note:
            Will get user id from environment.

        Args:
            db: database name
            pw: user password
        """
        self.user = getuser()
        self.db = db

        self.pw: str
        self._passw(pw)

    def _passw(self, pw: str | None = None) -> None:
        """Method for checking if user password exists.

        If the password is not given when instantiated,
        then ask for it from the user.
        """
        if pw is None:
            self.pw = getpass(f"Password for user {self.user}: ")
        else:
            self.pw = pw

    def select(self, sql: str) -> list[dict[str, Any]]:
        """Get data from Oracle database with fetchall method.

        Method for normal select SQL query. It will do a fetchall procedure.
        If it is too much data, then use the select_many method.

        Args:
            sql (str): the SQL query statement

        Returns:
            list[dict[str, Any]]: A list of dictionaries of every record, column names as keys.

        Raises:
            error: If the connection returns an error.
        """
        try:
            # create connection to database
            with oracledb.connect(
                user=self.user, password=self.pw, dsn=self.db
            ) as conn:
                # create cursor
                with conn.cursor() as cur:
                    # execute the select SQL query
                    cur.execute(sql)
                    # gets the column names
                    cols = [c[0].lower() for c in cur.description]
                    # gets the data as a list of tuples
                    rows = cur.fetchall()
                    # convert data from list of tuples to list of dictionaries
                    data = [dict(zip(cols, row, strict=False)) for row in rows]
        except oracledb.Error as error:
            raise error
        return data

    def update_or_insert(self, sql: str, update: list[tuple[Any, ...]]) -> None:
        """Update data or insert new data to Oracle database.

        Method to do either update or insert SQL query. It is important that
        the SQL query statement and the data column names and values come in
        correct order to each other.

        Args:
            sql (str): SQL query statement, insert or update.
            update (list[tuple[Any, ...]]): list of record values to insert or update.

        Raises:
            error: If the connection returns an error.
        """
        try:
            # create connection to database
            with oracledb.connect(
                user=self.user, password=self.pw, dsn=self.db
            ) as conn:
                # create cursor
                with conn.cursor() as cur:
                    # execute the update or insert statement to the database
                    if len(update) == 1:
                        cur.execute(sql, update[0])
                    else:
                        cur.executemany(sql, update)
                    # commit the changes in the database
                    conn.commit()
        except oracledb.Error as error:
            raise error

    def select_many(self, sql: str, batchsize: int) -> list[dict[str, Any]]:
        """Get data from Oracle database in batches with fetchmany method.

        Method for normal select SQL query. It will do a fetchmany procedure,
        which is preferred when there is a lot of data that needs to be fetched.

        Args:
            sql (str): the SQL query statement.
            batchsize (int): the size of rows per batch.

        Returns:
            list[dict[str, Any]]: A list of dictionaries of every record, column names as keys.

        Raises:
            error: If the connection returns an error.
        """
        try:
            # create connection to database
            with oracledb.connect(
                user=self.user, password=self.pw, dsn=self.db
            ) as conn:
                # create cursor
                with conn.cursor() as cur:
                    # execute the select SQL query
                    cur.execute(sql)
                    # gets the column names
                    cols = [c[0].lower() for c in cur.description]
                    # gets all the data in batches
                    data: list[dict[str, Any]] = []
                    while True:
                        rows = cur.fetchmany(batchsize)
                        if not rows:
                            break
                        else:
                            rows = [dict(zip(cols, row, strict=False)) for row in rows]
                            data.extend(rows)
        except oracledb.Error as error:
            raise error
        return data

    def close(self) -> None:
        """Close connection and delete the class attribute values."""
        del self.user
        del self.pw
        del self.db

    def __enter__(self) -> oracledb.Cursor:
        """Enter context manager mode.

        When entering context manager it will go straight to the cursor.

        Returns:
            oracledb.Cursor: the cursor
        """
        self.conn: oracledb.Connection = oracledb.connect(
            user=self.user, password=self.pw, dsn=self.db
        )  # Avoid Mypy complaining that oracledb is not fully typed
        cast(Any, self.conn).__enter__()
        self.cur: oracledb.Cursor = self.conn.cursor()
        cast(Any, self.cur).__enter__()
        return self.cur

    def __exit__(
        self,
        exc_type: None | type[BaseException],
        exc_value: None | BaseException,
        traceback: None | TracebackType,
    ) -> bool:
        """Exit the context manager mode.

        When exiting context manager, it closes both cursor and connection,
        as well as closing the class itself. All class attribute values
        will be deleted as well.
        """
        cast(Any, self.cur).__exit__(
            exc_type, exc_value, traceback
        )  # Avoid Mypy complaining that oracledb is not fully typed
        cast(Any, self.conn).__exit__(exc_type, exc_value, traceback)
        self.close()
        del self.cur
        del self.conn
        return True
