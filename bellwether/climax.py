import util.date_util as date_util
import init.db_init as db_init
import init.tushare_init as tushare_init
import util.type_util as type_util
import math

# _SHORT_DAYS = 1
# _LONG_DAYS = 30
# _RISE_THRESHOLD = 0.1


def _parse_float(text, default_value=0.0):
    return type_util.parse_float(text, default_value)


def _amount_analyze(data, short_days, long_days):
    short_total = 0.0
    long_total = 0.0
    early_amounts = []
    close_list = []

    i = 0

    for row in data:
        amount = _parse_float(row[0])
        close = _parse_float(row[2], -1.0)
        close_list.append(close)

        long_total += amount
        if i < short_days:
            short_total += amount
        else:
            early_amounts.append(amount)

        i += 1

    long_avg = long_total / long_days
    short_avg = short_total / short_days

    early_avg = (long_total - short_avg) / (long_days - short_days)
    early_amount_percent_variance = sum( ((v - early_avg) / early_avg) ** 2 for v in early_amounts )

    last_rise = 0.0
    if len(close_list) > 1 and close_list[0] > 0.0 and close_list[1] > 0.0:
        last_rise = (close_list[0] - close_list[1]) / close_list[1]

    return short_avg, long_avg, last_rise, math.sqrt(early_amount_percent_variance)


def process(short_days=1, long_days=30, date=date_util.now_date_str()):

    def _climax_test(short_avg, long_avg, data):
        max_climax = 100.0
        min_climax = 1.5
        climax = short_avg / long_avg
        if climax > max_climax or climax < min_climax:
            return False

        if len(data) < short_days:
            return False

        price_up = 0.1
        price_down = 0.1
        end_price = data[0][2]
        begin_price = data[-1][1]
        end_price0 = (data[-1][2])
        if end_price < begin_price * (1 - price_down):
            return False
        if end_price > end_price0 * (1 + price_up):
            return False

        threshold_climax = min_climax * 1.1
        for (i, d) in enumerate(data):
            if i >= short_days and d[0] > (long_avg * threshold_climax):
                return False
        return True

    def _tx0(handler0):
        def _tx(handler):
            try:
                stock_info = handler.fetch_all(
                    'SELECT ts_code, name, industry FROM %s' % (db_init.DB_TABLE_STOCK_BASIC, ))

                handler0.drop_table(db_init.DB_TABLE_CLIMAX_ANALYSIS)
                handler0.create_table(db_init.DB_TABLE_CLIMAX_ANALYSIS, [
                    'code TEXT', 'name TEXT', 'fast REAL', 'slow REAL', 'climax REAL', 'variance REAL', 'l_rise REAL', 'r_rise REAL',
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

                    if _climax_test(short_avg, long_avg, data):
                        handler0.insert_into_table(db_init.DB_TABLE_CLIMAX_ANALYSIS, [
                            code,
                            si[1],
                            round(short_avg / 10000, 2),
                            round(long_avg / 10000, 2),
                            round(short_avg / long_avg, 2),
                            round(res[3], 2),
                            round(last_rise * 100, 2),
                            round((data[0][2] - data[-1][2]) / (data[-1][2]) * 100, 2),
                            round(data[0][3], 2),
                            round(data[0][4], 2),
                            si[2]
                        ])

            except Exception as e:
                print(e)

        db_init.execute(db_init.DB_PATH_MAIN, _tx)

    db_init.execute(db_init.DB_PATH_BELLWETHER, _tx0)

