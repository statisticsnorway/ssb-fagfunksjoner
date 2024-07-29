import cx_Oracle as ora
from getpass import getpass, getuser
from typing import Any


class Oracle:
    """Class for working with a Oracle database with most common queries.
    
    This class support the most used sql queries to a table in a database.
    It gives us the possibilities to query multiple times since the class
    has the credidentials needed stored until the user closes the
    connection.
    
    Note:
        User must remember to the close method after final use.
    
    Attributes:
        user: user id
        db: database name
        pw: user password
        conn: database connection if using context manager
        cur: database cursor if using context manager
    """

    def __init__(self, db: str, pw: str = None):
        """The instanciation of the class.
        
        Note:
            Will get user id fra environments.
        
        Args:
            db: database name
            pw: user password
        """
        self.user = getuser()
        self.db = db
        self.pw = pw
        self._passw()

    def _passw(self):
        """Method for checking if user password exist
        
        If password is not given when instanciated,
        then ask for it to the user.
        """
        if self.pw is None:
            self.pw = getpass(f'Password for user {self.user}: ')

    def select(self, sql: str) -> list[dict[str, Any]]:
        """Gets data from Oracle database with fetchall method.
        
        Method for normal select sql query. It will do a fetchall procedure.
        If it is to much data, then use method select_many instead.
        
        Args:
            sql: the sql query statement
        
        Returns:
            A list of dictionary of every record, column names as keys.
        """
        try:
            # create connection to database
            with ora.connect(self.user+"/"+self.pw+"@"+self.db) as conn:
                # create cursor
                with conn.cursor() as cur:
                    # execute the select sql query
                    cur.execute(sql)
                    # gets the column names
                    cols = [c[0].lower() for c in cur.description]
                    # gets the data as a list of tuples
                    rows = cur.fetchall()
                    # convert data from list of tuples to list of dictionaries
                    data = [dict(zip(cols, row)) for row in rows]
        except ora.Error as error:
            raise error
        return data

    def update_or_insert(self, sql: str, update: list[tuple[Any, ...]]) -> None:
        """Updates data or insert new data to Oracle database.
        
        Method to do either update or insert sql query. It is important that
        the sql quary statement and the data column names and value comes in
        correct order to each other.
        
        Args:
            sql: sql query statement, insert or update.
            update: list of record values to insert or update.
        """
        try:
            # create connection to database
            with ora.connect(self.user+"/"+self.pw+"@"+self.db) as conn:
                # create cursor
                with conn.cursor() as cur:
                    # execute the update or insert statement to the database
                    if len(update) == 1:
                        cur.execute(sql, update[0])
                    else:
                        cur.executemany(sql, update)
                    # commit the changes in the database
                    conn.commit()
        except ora.Error as error:
            raise error
    
    def select_many(self, sql: str, batchsize: int) -> list[dict[str, Any]]:
        """Gets data from Oracle database in batches with fetchmany method.
        
        Method for normal select sql query. It will do a fetchmany procedure,
        which is prefered when theres a lot of data that need to be fetched.
        
        Args:
            sql: the sql query statement.
            batchsize: the size of rows per batch.
        
        Returns:
            A list of dictionary of every record, column names as keys.
        """
        try:
            # create connection to database
            with ora.connect(self.user+"/"+self.pw+"@"+self.db) as conn:
                # create cursor
                with conn.cursor() as cur:
                    # execute the select sql query
                    cur.execute(sql)
                    # gets the column names
                    cols = [c[0].lower() for c in cur.description]
                    # gets all the data in batches
                    data = []
                    while True:
                        rows = cur.fetchmany(batchsize)
                        if not rows:
                            break
                        else:
                            rows = [dict(zip(cols, row)) for row in rows]
                            data = data + rows
        except ora.Error as error:
            raise error
        return data

    def close(self) -> None:
        """Method for closing and deleting the class attribute values"""
        del self.user
        del self.pw
        del self.db
    
    def __enter__(self):
        """Dunder method for entering context manager mode.
        
        When entering context manager it will go straigt to the cursor.
        
        Returns:
            the cursor
        """
        self.conn = ora.connect(self.user+"/"+self.pw+"@"+self.db)
        self.conn.__enter__()
        self.cur = self.conn.cursor()
        self.cur.__enter__()
        return self.cur
    
    def __exit__(self, exc_type, exc_value, traceback):
        """Dunder method for exiting context manager mode.
        
        when exiting context manager, it closes both cursor and connection,
        as well as closing the class itself. All class attribute values
        will be deleted as well.
        """
        self.cur.__exit__(exc_type, exc_value, traceback)
        self.conn.__exit__(exc_type, exc_value, traceback)
        self.close()
        del self.cur
        del self.conn


class OraError(ora.Error):
    """An Error class so we can raise our database error messages.
    """
    pass