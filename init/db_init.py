import os
import init.db as db

_DB_CONNECTIONS = {}

DB_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data')
DB_PATH_MAIN = os.path.join(DB_DIR, 'main.db')
DB_PATH_BELLWETHER = os.path.join(DB_DIR, 'bellwether.db')

DB_TABLE_INDEX_BASIC = 'index_basic'
DB_TABLE_STOCK_BASIC = 'stock_basic'
# DB_TABLE_UPDATE_DATE = 'update_date'
DB_TABLE_STOCK_DAILY = 'stock_daily'
DB_TABLE_CLIMAX_ANALYSIS = 'climax_analysis'
DB_TABLE_BOTTOM_REVERSAL = 'bottom_reversal'
DB_TABLE_LIMIT_UP = 'limit_up'
DB_TABLE_FLUCTUATE = 'fluctuate'
DB_TABLE_LOW_CLIMAX = 'low_climax'
DB_TABLE_TT = 'tt'


def get_connection(path):
    """ 
    因为多线程问题，主线程缓存了 Connection; 子线程应创建自己的 Connection
    """
    conn = _DB_CONNECTIONS.get(path)
    if conn is None:
        conn = db.Connection(path)  # , check_same_thread=False)
        _DB_CONNECTIONS[path] = conn
    return conn


def execute(path, transaction):
    """
    在主线程中使用，子线程应创建自己的 Connection
    """
    conn = get_connection(path)
    handler = conn.new_handler()
    handler.execute_transaction(transaction)
    handler.close()


def db_execute_once(path, transaction):
    conn = db.Connection(path)
    handler = conn.new_handler()
    handler.execute_transaction(transaction)
    handler.close()
    conn.close()


def _inner_init():
    if not os.path.exists(DB_DIR):
        os.mkdir(DB_DIR)
    # db_execute(DB_PATH_MAIN, lambda handler: handler.create_text_table(DB_TABLE_UPDATE_DATE,
    # ['target', 'date'], table_constraint='PRIMARY KEY(target)'))


_inner_init()
