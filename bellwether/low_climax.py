import sys

import util.date_util as date_util
import init.db_init as db_init
import init.tushare_init as tushare_init


def _amount_analyze(data, short_days, long_days):
    short_total = 0.0
    long_total = 0.0
    close_list = []

    i = 0

    for row in data:
        amount = row[0]
        close = row[2]
        close_list.append(close)

        long_total += amount
        if i < short_days:
            short_total += amount

        i += 1

    long_avg = long_total / long_days
    short_avg = short_total / short_days

    last_rise = 0.0
    if len(close_list) > 1 and close_list[0] > 0.0 and close_list[1] > 0.0:
        last_rise = (close_list[0] - close_list[1]) / close_list[1]

    return short_avg, long_avg, last_rise


def process(short_days=1, long_days=60, date=date_util.now_date_str()):

    def _test(short_avg, long_avg, data):
        min_climax = 1.5
        climax = short_avg / long_avg
        if climax < min_climax:
            return False, 0

        if len(data) < short_days:
            return False, 0

        min_price = sys.float_info.max
        # max_price = sys.float_info.min
        for d in data:
            if d[2] < min_price:
                min_price = d[2]
            # if d[2] > max_price:
            #    max_price = d[2]

        price_up = 0.05
        # price_down = 0.05
        end_price = data[0][2]
        if end_price > min_price * (1.0 + price_up):
            return False, 0

        return True, (end_price - min_price) / min_price

    def _tx0(handler0):
        def _tx(handler):
            try:
                stock_info = handler.fetch_all(
                    'SELECT ts_code, name, industry FROM %s' % (db_init.DB_TABLE_STOCK_BASIC, ))

                handler0.drop_table(db_init.DB_TABLE_LOW_CLIMAX)
                handler0.create_table(db_init.DB_TABLE_LOW_CLIMAX, [
                    'code TEXT', 'name TEXT', 'fast REAL', 'slow REAL', 'climax REAL', 'l_rise REAL', 'l_m_rise REAL',
                    'pe REAL', 'pb REAL', 'industry TEXT'])

                for si in stock_info:
                    code = tushare_init.baostock_code(si[0])
                    data = handler.fetch_all('SELECT amount, open, close, peTTM, pbMRQ FROM %s WHERE date <= ? '
                                             'AND code = ? AND amount <> \'\' ORDER BY date DESC LIMIT %d'
                                             % (db_init.DB_TABLE_STOCK_DAILY, long_days),
                                             [date, code])

                    res = _amount_analyze(data, short_days, long_days)
                    short_avg = res[0]
                    long_avg = res[1]
                    last_rise = res[2]
                    if short_avg <= 0 or long_avg <= 0:
                        continue

                    res1 = _test(short_avg, long_avg, data)
                    if res1[0]:
                        handler0.insert_into_table(db_init.DB_TABLE_LOW_CLIMAX, [
                            code,
                            si[1],
                            round(short_avg / 10000, 2),
                            round(long_avg / 10000, 2),
                            round(short_avg / long_avg, 2),
                            round(last_rise * 100, 2),
                            round(res1[1] * 100, 2),
                            round(data[0][3], 2),
                            round(data[0][4], 2),
                            si[2]
                        ])

            except Exception as e:
                print(e)

        db_init.execute(db_init.DB_PATH_MAIN, _tx)

    db_init.execute(db_init.DB_PATH_BELLWETHER, _tx0)

