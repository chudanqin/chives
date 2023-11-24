import db_init
import tushare_init
import daily
import date_util

_SHORT_DAYS = 1
_LONG_DAYS = 20
_RISE_THRESHOLD = 0.2


def _parse_float(text, default_value=0.0):
    try:
        return float(text)
    except Exception as e:
        print('parse float error(%s), %s' % (text, e))
        return default_value


def _amount_analyze(data, short_days, long_days):
    short_total = 0.0
    long_total = 0.0
    close_list = []

    i = 0

    for row in data:
        amount = _parse_float(row[0])
        close = _parse_float(row[2], -1.0)
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

    return (short_avg, long_avg, last_rise)


def amount_surge(short_days, long_days, threshold_test, date=date_util.now_date_str()):
    def _tx0(handler0):
        def _tx(handler):
            try:
                stock_info = handler.fetch_all(
                    'SELECT ts_code, name, industry FROM %s' % (db_init.DB_TABLE_STOCK_BASIC))

                handler0.drop_table(db_init.DB_TABLE_AMOUNT_ANALYSIS)
                handler0.create_table(db_init.DB_TABLE_AMOUNT_ANALYSIS, [
                    'code TEXT', 'name TEXT', 'fast REAL', 'slow REAL', 'surge REAL', 'l_rise REAL', 'r_rise', 'industry TEXT'])

                for si in stock_info:
                    code = tushare_init.baostock_code(si[0])
                    data = handler.fetch_all('SELECT amount, open, close FROM %s WHERE date <= ? AND code = ? ORDER BY date DESC LIMIT %d' % (
                        db_init.DB_TABLE_STOCK_DAILY, long_days), [date, code])

                    res = _amount_analyze(data, short_days, long_days)
                    short_avg = res[0]
                    long_avg = res[1]
                    last_rise = res[2]
                    if short_avg <= 0 or long_avg <= 0:
                        continue

                    if threshold_test(short_avg, long_avg, data):
                        handler0.insert_into_table(db_init.DB_TABLE_AMOUNT_ANALYSIS, [
                            code,
                            si[1],
                            round(short_avg / 10000, 2),
                            round(long_avg / 10000, 2),
                            round(short_avg / long_avg, 2),
                            round(last_rise * 100, 2),
                            round((data[0][2] - data[-1][2]) / (data[-1][2]) * 100, 2),
                            si[2]
                        ])

            except Exception as e:
                print(e)

        db_init.execute(db_init.DB_PATH_MAIN, _tx)

    db_init.execute(db_init.DB_PATH_BELLWETHER, _tx0)


def _amount_surge_test(short_avg, long_avg, data):
    sg = short_avg / long_avg
    if sg < 3:
        return False
    if len(data) < _SHORT_DAYS:
        return False

    end_price = _parse_float(data[0][2])
    begin_price = _parse_float(data[_SHORT_DAYS - 1][1])
    end_price0 = _parse_float((data[-1][2]))
    if end_price <= begin_price:
        return False
    if end_price > end_price0 * (1 + _RISE_THRESHOLD):
        return False

    for (i, d) in enumerate(data):
        if i >= _SHORT_DAYS and _parse_float(d[0]) > (long_avg * 1.5):
            return False
    return True


def tj():
    print("tj begin...")
    # daily.update_main_index_basic()
    # daily.update_stock_basic()
    daily.update_stock_daily_from_pool()
    amount_surge(_SHORT_DAYS, _LONG_DAYS, _amount_surge_test)
    print("tj end...")


tj()
