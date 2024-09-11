import util.date_util as date_util
import init.db_init as db_init
import init.tushare_init as tushare_init


def funds_flow_by_industry(begin_date='2015-01-01', end_date=date_util.now_date_str()):
    def _tx0(handler0):

        def _tx(handler1):
            try:
                stock_info = handler1.fetch_all(
                    'SELECT ts_code, industry FROM %s' % (db_init.DB_TABLE_STOCK_BASIC))
                stock_map = {}

                for si in stock_info:
                    code = tushare_init.baostock_code(si[0])
                    stock_map[code] = si[1]

                table_name = 'industry_flow'
                handler0.drop_table(table_name)
                handler0.create_table(
                    table_name, ['tag TEXT', 'date TEXT', 'amount REAL', 'p REAL'],
                    table_constraint='PRIMARY KEY(tag, date)')

                all_dates = list(map(lambda row: row[0], handler1.fetch_all(
                    'SELECT DISTINCT(date) FROM stock_daily WHERE date >= ? and date <= ?',
                    [begin_date, end_date])))

                for date in all_dates:
                    data = handler1.fetch_all('SELECT code, amount FROM stock_daily '
                                              'WHERE date = ? '
                                              'AND cast(turn as decimal) > 0.0 ',
                                              [date])
                    total_amount = 0.0
                    industry_amount_map = {}
                    for stock in data:
                        s_code = stock[0]
                        s_amount = stock[1]
                        total_amount = total_amount + s_amount
                        s_industry = stock_map[s_code]
                        if s_industry is not None:
                            i_amount = industry_amount_map.get(s_industry)
                            if i_amount is None:
                                i_amount = 0.0
                            else:
                                i_amount = i_amount + s_amount
                            industry_amount_map[s_industry] = i_amount

                    if total_amount == 0.0:
                        continue

                    for ind in industry_amount_map:
                        handler0.insert_into_table(table_name, [
                            ind, date, round(industry_amount_map[ind] / 100000000, 2), round(industry_amount_map[ind] / total_amount, 4)
                        ])

                    print('industry flow(%s) done' % date)

            except Exception as e:
                print(e)

        db_init.execute(db_init.DB_PATH_MAIN, _tx)

    db_init.execute(db_init.DB_PATH_BELLWETHER, _tx0)


def process():
    now_date = date_util.now_date_str()
    begin_date = date_util.date_str_from_day_delta(now_date, day_delta=-5)
    funds_flow_by_industry(begin_date, now_date)

