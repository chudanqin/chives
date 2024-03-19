import sqlite3

"""
sqlite wrapper
"""


class Connection(object):
    def __init__(self, path, **kw):
        self.db_conn = sqlite3.connect(path, **kw)

    def new_handler(self):
        return Handler(self.db_conn)

    def close(self):
        if self.db_conn is not None:
            self.db_conn.close()
            self.db_conn = None


class Handler(object):
    def __init__(self, connection):
        self.cursor = connection.cursor()

    # def __del__(self):
    #     self.close()

    def close(self):
        if self.cursor is not None:
            self.cursor.close()
            self.cursor = None

    def execute_transaction(self, transaction):
        self.cursor.execute('BEGIN;')
        transaction(self)
        self.cursor.execute('COMMIT;')

    def execute(self, sql, parameters=None):
        if parameters is None:
            parameters = []
        self.cursor.execute(sql, parameters)

    def fetch_all(self, sql, parameters=None):
        if parameters is None:
            parameters = []
        parameters = list(map(lambda x: str(x), parameters))
        self.execute(sql, parameters)
        return self.cursor.fetchall()

    def fetch_first(self, sql, parameters=None):
        if parameters is None:
            parameters = []
        values = self.fetch_all(sql, parameters)
        if len(values) > 0:
            return values[0]
        return None

    def create_text_table(self, table_name, columns, table_constraint=None):
        """
        create a table whose column types are all 'TEXT'
        columns: [str]
        """

        col_def = list(map(lambda x: x + ' TEXT', columns))
        self.create_table(table_name, col_def, table_constraint)

    def create_table(self, table_name, column_defs, if_not_exists=True, table_constraint=None):
        col_def = ', '.join(column_defs)
        sql = 'CREATE TABLE '
        if if_not_exists:
            sql = sql + 'IF NOT EXISTS '
        sql = sql + table_name + ' (' + col_def
        if table_constraint is not None:
            sql = sql + ', ' + table_constraint + ')'
        else:
            sql = sql + ')'
        self.cursor.execute(sql)

    def create_index(self, table_name, index_name, columns, unique=True):
        sql = 'CREATE'
        if unique:
            sql = sql + ' UNIQUE'
        sql = sql + ' INDEX IF NOT EXISTS ' + index_name + ' ON ' + table_name + '(' + (', '.join(columns)) + ')'

    def insert_into_table(self, table_name, values, keys=None, or_action=None):
        sql = 'INSERT'
        if or_action is not None:
            sql = sql + ' OR ' + or_action
        sql = sql + ' INTO ' + table_name
        if keys is not None:
            sql = sql + ' (' + ', '.join(keys) + ')'
        sql = sql + ' VALUES(' + (', '.join(['?'] * len(values))) + ')'
        self.cursor.execute(sql, values)

    def update_table(self, table_name, kvs, where):
        sql = 'UPDATE ' + table_name + ' SET '
        set_statments = []
        values = []
        for k, v in kvs.items():
            set_statments.append(k + '=?')
            values.append(v)
        sql += ', '.join(set_statments)
        if where is not None:
            sql += ' WHERE ' + where
        self.cursor.execute(sql, values)

    def delete_table(self, table_name, where=None):
        sql = 'DELETE FROM ' + table_name
        if where is not None:
            sql = sql + ' ' + where
        self.cursor.execute(sql)

    def drop_table(self, table_name):
        sql = 'DROP TABLE IF EXISTS ' + table_name
        self.cursor.execute(sql)
