import date_util
import db_init
import tushare_init


"""
某段时间内的波动率
初值：begin_date
终值：end_date
f: 终值与初值的差值百分比
f_max: 最大值与终值的差值百分比
f_min: 最小值与终值的差值百分比
"""


def _process(begin_date, end_date):
    def _tx0(handler0):
        def _tx(handler):
            try:
                stock_info = handler.fetch_all(
                    'SELECT ts_code, name, industry FROM %s' % (db_init.DB_TABLE_STOCK_BASIC,))

                handler0.drop_table(db_init.DB_TABLE_FLUCTUATE)
                handler0.create_table(db_init.DB_TABLE_FLUCTUATE, [
                    'code TEXT', 'name TEXT',
                    'begin_close', 'end_close', 'min_close', 'max_close',
                    'f REAL', 'f_max REAL', 'f_min REAL',
                    'pe REAL', 'pb REAL', 'industry TEXT'
                ])

                for si in stock_info:
                    code = tushare_init.baostock_code(si[0])
                    # find the data close to the day of begin_date
                    first_data = handler.fetch_first('SELECT close FROM %s WHERE date <= ? '
                                                     'AND code = ? AND amount <> \'\' ORDER BY date desc LIMIT 1'
                                                     % (db_init.DB_TABLE_STOCK_DAILY,),
                                                     [begin_date, code])
                    # find the data close to the day of end_date
                    last_data = handler.fetch_first('SELECT close, peTTM, pbMRQ FROM %s WHERE date <= ? '
                                                    'AND code = ? AND amount <> \'\' ORDER BY date desc LIMIT 1'
                                                    % (db_init.DB_TABLE_STOCK_DAILY,),
                                                    [end_date, code])
                    # find the min/max data between begin_date and end_date
                    min_max_data = handler.fetch_first('SELECT min(close), max(close) FROM %s '
                                                       'WHERE date >= ? AND date <= ?'
                                                       'AND code = ? AND amount <> \'\''
                                                       % (db_init.DB_TABLE_STOCK_DAILY,),
                                                       [begin_date, end_date, code])

                    if first_data is not None and last_data is not None:
                        # rise/fall from begin_date to end_date
                        diff = (last_data[0] - first_data[0]) / first_data[0]
                        # max rise from begin_date to end_date
                        diff_max = (min_max_data[1] - last_data[0]) / last_data[0]
                        # max fall from begin_date to end_date
                        diff_min = (min_max_data[0] - last_data[0]) / last_data[0]

                        handler0.insert_into_table(db_init.DB_TABLE_FLUCTUATE, [
                            code,
                            si[1],
                            first_data[0],
                            last_data[0],
                            min_max_data[0],
                            min_max_data[1],
                            round(100 * diff, 2),
                            round(100 * diff_max, 2),
                            round(100 * diff_min, 2),
                            last_data[1],
                            last_data[2],
                            si[2]
                        ])

            except Exception as e:
                print(e)

        db_init.execute(db_init.DB_PATH_MAIN, _tx)

    db_init.execute(db_init.DB_PATH_BELLWETHER, _tx0)


def process():
    now_date = date_util.now_date_str()
    _process(date_util.date_str_from_day_delta(now_date, day_delta=-60), now_date)
