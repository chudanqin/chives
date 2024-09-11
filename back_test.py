# import init.db_init as db_init
# import init.tushare_init as tushare_init
# import date_util
#
#
# def first_amount_surge(short_days, long_days, threshold, begin_date, end_date = date_util.now_date_str()):
#
#     def _tx(handler):
#         try:
#             stock_info = handler.fetch_all(
#                 'SELECT ts_code, name FROM %s' % (db_init.DB_TABLE_STOCK_BASIC, ))
#
#             handler.drop_table(db_init.DB_TABLE_CLIMAX_ANALYSIS)
#             handler.create_table(db_init.DB_TABLE_CLIMAX_ANALYSIS, [
#                                       'code TEXT', 'name TEXT', 'fast REAL', 'slow REAL', 'surge REAL'])
#
#             for si in stock_info:
#                 code = tushare_init.baostock_code(si[0])
#                 data = handler.fetch_all('SELECT amount, open, close FROM %s WHERE code = ? AND date >= ? AND date <= ? ORDER BY date DESC' % (
#                     db_init.DB_TABLE_STOCK_DAILY, begin_date), [code, begin_date, end_date])
#
#                 res = _amount_analyze(short_days, long_days, data)
#                 if res[0] <= 0 or res[1] <= 0:
#                     continue
#
#                 sg = res[0] / res[1]
#                 if sg >= threshold:
#                     handler.insert_into_table(db_init.DB_TABLE_AMOUNT_ANALYSIS, [
#                                               code, si[1], res[0], res[1], sg])
#
#         except Exception as e:
#             print(e)
#
#     db_init.execute(db_init.DB_PATH_MAIN, _tx)