import cx_Oracle as ora
from getpass import getpass, getuser


class Oracle:

    def __init__(self, db: str, pw: str = None):
        self.user = getuser()
        self.db = db
        self.pw = pw
        self._passw()

    def _passw(self):
        if self.pw is None:
            self.pw = getpass(f'Oracle passord for bruker {self.user}: ')

    def select(self, sql: str) -> pd.DataFrame:
        """Henter data fra Oracle database"""
        try:
            # Oppretter connection til databasen
            with ora.connect(self.user+"/"+self.pw+"@"+self.db) as conn:
                # Oppretter cursoren i databasen
                with conn.cursor() as cur:
                    # Foreta spørringen
                    cur.execute(sql)
                    # Henter kolonne navnene
                    cols = []
                    for c in cur.description:
                        cols.append(c[0].lower())
                    # Henter ut dataene fra spørringen
                    data = cur.fetchall()
                    print('Data fetched successfully!')
        except ora.Error as error:
            print(error)
        return pd.DataFrame(data, columns=cols)

    def update(self, sql: str, update: list):
        """Oppdaterer data i Oracle database"""
        # Oppretter kobling med Oracle database
        try:
            with ora.connect(self.user+"/"+self.pw+"@"+self.db) as conn:
                with conn.cursor() as cur:
                    if len(update) == 1:
                        cur.execute(sql, update[0])
                    else:
                        cur.executemany(sql, update)
                    conn.commit()
                    print('Update successful!')
        except ora.Error as error:
            print(error)

    @staticmethod
    def converter(df: pd.DataFrame, cols: list = None) -> list:
        if cols is None:
            return list(df.itertuples(index=False, name=None))
        else:
            return list(df[cols].itertuples(index=False, name=None))

    def close(self):
        del self.user
        del self.pw
        del self.db