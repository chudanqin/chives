import baostock as bs

import date_util
import db_init
import tushare_init

_stock_daily_query_parameters \
    = 'code, date, open, high, low, close, preclose, volume, amount, adjustflag, turn, tradestatus, pctChg, peTTM, pbMRQ, psTTM, pcfNcfTTM, isST'


def _stock_daily_table_column_define():
    return [
        'code TEXT',
        'date TEXT',
        'open REAL',
        'high REAL',
        'low REAL',
        'close REAL',
        'preclose REAL',  # 昨日收盘价
        'volumn INTEGER',  # 成交量/股
        'amount REAL',  # 成交额/元
        'adjustflag INTEGER',  # 复权状态/不复权、前复权、后复权
        'turn REAL',  # 换手率
        'tradestatus INTEGER',  # 1：正常交易 0：停牌
        'pctChg REAL',  # 涨跌幅（百分比）
        'peTTM REAL',
        'pbMRQ REAL',  # 市净率
        'psTTM REAL',
        'pcfNcfTTM REAL',  # 滚动市现率
        'isST INTEGER'
    ]


# def _tryCatch(tx):
#     try:
#         tx()
#     except Exception as e:
#         print(e)

"""
市场代码	说明
MSCI	MSCI指数
CSI	中证指数
SSE	上交所指数
SZSE	深交所指数
CICC	中金所指数
SW	申万指数
OTH	其他指数
"""

_first_time_update_index_basic = True


def update_index_basic(market):
    try:
        data = tushare_init.API.index_basic(market=market)

        def _tx(handler):
            try:
                table_name = db_init.DB_TABLE_INDEX_BASIC

                global _first_time_update_index_basic
                if _first_time_update_index_basic:
                    handler.drop_table(table_name)
                    handler.create_text_table(table_name, data.columns.values, table_constraint='PRIMARY KEY(ts_code)')
                    _first_time_update_index_basic = False

                for i in data.index:
                    row_data = data.loc[i].array
                    handler.insert_into_table(table_name, row_data, or_action='REPLACE')
            except Exception as e:
                print(e)

        db_init.execute(db_init.DB_PATH_MAIN, _tx)

    except Exception as e:
        print(e)


def update_main_index_basic():
    update_index_basic('MSCI')
    update_index_basic('CSI')
    update_index_basic('SSE')
    update_index_basic('SZSE')
    update_index_basic('CICC')


def update_stock_basic():
    try:
        data = tushare_init.API.stock_basic(exchange='', list_status='L')

        def _tx(handler):
            try:
                table_name = db_init.DB_TABLE_STOCK_BASIC
                handler.drop_table(table_name)
                handler.create_text_table(table_name, data.columns.values, table_constraint='PRIMARY KEY(ts_code)')
                for i in data.index:
                    row_data = data.loc[i].array
                    handler.insert_into_table(
                        table_name, row_data, or_action='REPLACE')
            except Exception as e:
                print(e)

        db_init.execute(db_init.DB_PATH_MAIN, _tx)

    except Exception as e:
        print(e)


def _update_stock_daily(code, begin_date, end_date=None):
    if begin_date is None:
        begin_date = '2015-01-01'
    if end_date is None:
        end_date = date_util.now_date_str()

    rs = bs.query_history_k_data_plus(code,
                                      _stock_daily_query_parameters,
                                      start_date=begin_date,
                                      end_date=end_date,
                                      frequency='d',
                                      adjustflag='2')
    if rs.error_code != '0':
        print('query_history_k_data_plus(%s) respond code: %s, msg: %s' %
              (code, rs.error_code, rs.error_msg))
        return

    def _tx(handler):
        try:
            while (rs.error_code == '0') & rs.next():
                handler.insert_into_table(
                    db_init.DB_TABLE_STOCK_DAILY, rs.get_row_data(), or_action='REPLACE')
        except Exception as e:
            print(e)

    db_init.execute(db_init.DB_PATH_MAIN, _tx)


def update_stock_daily_from_pool():
    #### 登陆 baostock 系统 ####
    lg = bs.login()
    if lg.error_code != '0':
        print('baostock login respond code: %s, msg: %s' %
              (lg.error_code, lg.error_msg))
        return

    # db_init.execute(db_init.DB_PATH_MAIN, lambda handler: handler.create_text_table(
    #     db_init.DB_TABLE_UPDATE_DATE, ['target', 'date'], table_constraint='PRIMARY KEY(target)'))
    db_init.execute(db_init.DB_PATH_MAIN, lambda handler: handler.create_table(
        db_init.DB_TABLE_STOCK_DAILY, _stock_daily_table_column_define(), table_constraint='PRIMARY KEY(code, date)'))

    stock_info = []

    def _tx(handler):
        try:
            nonlocal stock_info
            stock_info = handler.fetch_all('SELECT ts_code from %s' % (db_init.DB_TABLE_STOCK_BASIC))
        except Exception as e:
            print(e)

    db_init.execute(db_init.DB_PATH_MAIN, _tx)

    stock_codes = list(map(lambda si: tushare_init.baostock_code(si[0]), stock_info))
    # main_indices = ['sh.000001', 'sz.399001', 'sz.399006']
    # for i in main_indices:
    #     stock_codes.append(i)

    curr_date = date_util.now_date_str()
    for sc in stock_codes:
        revised_code = sc  # tushare_init.baostock_code(si[0])
        begin_date = _get_latest_date(revised_code)
        if begin_date is not None:
            if begin_date >= curr_date:
                print('stock(%s) updated already on %s' % (revised_code, curr_date))
                continue
            begin_date = date_util.date_str_from_day_delta(begin_date)
        _update_stock_daily(revised_code, begin_date, curr_date)
        print('stock(%s) daily done!' % (revised_code, ))

    bs.logout()
    print('baostock logout')


def _get_latest_date(code):
    row = None

    def _tx1(handler):
        nonlocal row
        row = handler.fetch_first('SELECT MAX(date) from %s WHERE code = ?' % (db_init.DB_TABLE_STOCK_DAILY), [code])

    db_init.execute(db_init.DB_PATH_MAIN, _tx1)

    if row is not None and len(row) == 1:
        return row[0]
    return None

# update_stock_basic()
# udpate_stock_daily_from_pool()
# update_main_index_basic()
