import daily
from bellwether import climax
from bellwether import limit_up
from bellwether import fluctuate
from bellwether import value_band
from bellwether import funds_flow
from bellwether import industry_flow


# def guess_bottom_reversal(date=date_util.now_date_str(), days=20):
#     def _tx0(handler0):
#         def _tx(handler):
#             try:
#                 stock_info = handler.fetch_all(
#                     'SELECT ts_code, name, industry FROM %s' % (db_init.DB_TABLE_STOCK_BASIC, ))
#
#                 handler0.drop_table(db_init.DB_TABLE_BOTTOM_REVERSAL)
#                 handler0.create_table(db_init.DB_TABLE_BOTTOM_REVERSAL, [
#                     'code TEXT', 'name TEXT', 'highest REAL', 'lowest REAL', 'l_rise REAL', 'r_rise', 'industry TEXT'])
#
#                 for si in stock_info:
#                     code = tushare_init.baostock_code(si[0])
#                     data = handler.fetch_all('SELECT amount, open, close FROM %s WHERE date <= ? AND code = ? ORDER BY date DESC LIMIT %d' % (
#                         db_init.DB_TABLE_STOCK_DAILY, long_days), [date, code])
#
#                     res = _amount_analyze(data, short_days, long_days)
#                     short_avg = res[0]
#                     long_avg = res[1]
#                     last_rise = res[2]
#                     if short_avg <= 0 or long_avg <= 0:
#                         continue
#
#                     if threshold_test(short_avg, long_avg, data):
#                         handler0.insert_into_table(db_init.DB_TABLE_AMOUNT_ANALYSIS, [
#                             code,
#                             si[1],
#                             round(short_avg / 10000, 2),
#                             round(long_avg / 10000, 2),
#                             round(short_avg / long_avg, 2),
#                             round(last_rise * 100, 2),
#                             round((data[0][2] - data[-1][2]) / (data[-1][2]) * 100, 2),
#                             si[2]
#                         ])
#
#             except Exception as e:
#                 print(e)
#
#         db_init.execute(db_init.DB_PATH_MAIN, _tx)
#
#     db_init.execute(db_init.DB_PATH_BELLWETHER, _tx0)


def tj():
    print("tj begin...")
    # daily.update_main_index_basic() #
    daily.update_stock_basic()
    daily.update_stock_daily_from_pool()
    climax.process()
    limit_up.process()
    fluctuate.process()
    value_band.process()
    funds_flow.process()
    industry_flow.process()
    print("tj end...")


tj()
