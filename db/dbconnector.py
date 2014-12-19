import sqlite3
import re


class DBConnector():

    def __init__(self, dbfile):
        self.conn = sqlite3.connect(dbfile)

    def execute_query(self, query):
        result = None
        create = re.compile('create', re.IGNORECASE)
        select = re.compile('select', re.IGNORECASE)
        insert = re.compile('insert', re.IGNORECASE)
        update = re.compile('update', re.IGNORECASE)
        c = self.conn.cursor()
        c.execute(query)
        if re.match(create, query) or re.match(insert, query) or re.match(update, query):
            self.conn.commit()
        elif re.match(select, query):
            result = c.fetchall()
        else:
            print "Query not supported. {0}".format(query)
            self.conn.commit()
        if result:
            return c.lastrowid, result
        else:
            return c.lastrowid

    def close_conn(self):
        self.conn.close()
