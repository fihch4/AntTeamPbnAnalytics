"""
pip install pymysql
"""
import pymysql

class PyMySQLi:
    _connection = None

    def __init__(self, ip, username, password, database='®', port=3306):
        try:
            self._connection = pymysql.connect(host=ip,
                                               user=username,
                                               password=password,
                                               port=port,
                                               database=database,
                                               ssl = {'key': 'whatever'})
        except pymysql.err.OperationalError as e:
            print(f"pymysql.err.OperationalError: {e}")

    def query(self, sql, args=None, type_work="one"):
        cursor = None
        self.type_work = type_work
        if self._connection != None:
            cursor = self._connection.cursor()
            if self.type_work == "one":
                cursor.execute(sql, args)
            elif self.type_work == "many":
                cursor.executemany(sql, args)
            return cursor
        elif self._connection is None:
            return "Error"
        return cursor

    def fetch(self, sql, *args):
        result = {"rows": []}
        cursor = self.query(sql, args)
        if cursor != None and cursor != "Error":
            rows = cursor.fetchall()
            result["rows"] = rows
            cursor.close()
            return result
        elif cursor == 'Error':
            return "Error"
