import cx_Oracle as ora
from getpass import getpass, getuser
from typing import Any


class Oracle:

    def __init__(self, db: str, pw: str = None):
        self.user = getuser()
        self.db = db
        self.pw = pw
        self._passw()

    def _passw(self):
        if self.pw is None:
            self.pw = getpass(f'Password for user {self.user}: ')

    def select(self, sql: str) -> list[dict[str, Any]]:
        """Gets data from Oracle database with fetchall method"""
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

    def update_or_insert(self, sql: str, update: list[tuple]) -> None:
        """Updates data or insert new data to Oracle database"""
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
        """Gets data from Oracle database in batches with fetchmany method"""
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
        del self.user
        del self.pw
        del self.db
    
    def __enter__(self):
        self.conn = ora.connect(self.user+"/"+self.pw+"@"+self.db)
        self.conn.__enter__()
        self.cur = self.conn.cursor()
        self.cur.__enter__()
        return self.cur
    
    def __exit__(self, exc_type, exc_value, traceback):
        self.cur.__exit__(exc_type, exc_value, traceback)
        self.conn.__exit__(exc_type, exc_value, traceback)
        self.close()
        del self.cur
        del self.conn


class OraError(ora.Error):
    pass