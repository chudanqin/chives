import date_util
import db_init


# 50/300/500/1000/2000/trailing-x
def funds_flow_by_value(begin_date='2015-01-01', end_date=date_util.now_date_str()):
    def _tx0(handler0):

        def _tx(handler1):
            try:
                # stock_info = handler1.fetch_all(
                #     'SELECT ts_code, name, industry FROM %s' % (db_init.DB_TABLE_STOCK_BASIC))
                # stock_map = {}
                #
                # for si in stock_info:
                #     code = tushare_init.baostock_code(si[0])
                #     stock_map[code] = (si[1], si[2])

                table_name = 'funds_flow'
                handler0.drop_table(table_name)
                handler0.create_table(
                    table_name, ['tag TEXT', 'date TEXT', 'amount REAL', 'mv REAL', 'amount_diff REAL', 'mv_diff REAL'],
                    table_constraint='PRIMARY KEY(tag, date)')

                all_dates = list(map(lambda row: row[0], handler1.fetch_all(
                    'SELECT DISTINCT(date) FROM stock_daily WHERE date >= ? and date <= ?',
                    [begin_date, end_date])))

                zz50 = zz300 = zz500 = zz1000 = zz2000 = zzt = zza = (0.0, 0.0, 0.0, 0.0)

                def _x(dt, f, t, prev_amount, prev_market_value):
                    assert f < t
                    dtl = len(dt)
                    if t > dtl - 1:
                        t = dtl - 1
                    s0 = 0.0
                    s1 = 0.0
                    for j in range(f, t):
                        d = dt[j]
                        s0 = s0 + d[0]
                        s1 = s1 + d[1]
                    return s0, s1, s0 - prev_amount if prev_amount > 0.0 else 0.0, s1 - prev_market_value if prev_market_value > 0 else 0.0

                def _y(tag, zzx):
                    handler0.insert_into_table(table_name,
                                               [tag, date,
                                                round(zzx[0] / 100000000, 1),
                                                round(zzx[1] / 100000000, 1),
                                                round(zzx[2] / 100000000, 1),
                                                round(zzx[3] / 100000000, 1)],
                                               or_action='REPLACE')

                for date in all_dates:
                    data = handler1.fetch_all('SELECT amount, amount/turn*100 mv FROM stock_daily '
                                              'WHERE date = ? '
                                              'AND cast(turn as decimal) > 0.0 '
                                              'AND cast(amount as decimal) > 0.0 '
                                              'ORDER BY mv DESC', [date])
                    zz50 = _x(data, 0, 50, zz50[0], zz50[1])
                    zz300 = _x(data, 50, 300, zz300[0], zz300[1])
                    zz500 = _x(data, 300, 800, zz500[0], zz500[1])
                    zz1000 = _x(data, 800, 1800, zz1000[0], zz1000[1])
                    zz2000 = _x(data, 1800, 3800, zz2000[0], zz2000[1])
                    zzt = _x(data, len(data) - 500, len(data) - 1, zzt[0], zzt[1])
                    zza = _x(data, 0, len(data) - 1, zza[0], zza[1])
                    _y('zz50', zz50)
                    _y('zz300', zz300)
                    _y('zz500', zz500)
                    _y('zz1000', zz1000)
                    _y('zz2000', zz2000)
                    _y('zzt', zzt)
                    _y('zza', zza)
                    print('zzx(%s) done' % date)

            except Exception as e:
                print(e)

        db_init.execute(db_init.DB_PATH_MAIN, _tx)

    db_init.execute(db_init.DB_PATH_BELLWETHER, _tx0)


def process():
    now_date = date_util.now_date_str()
    begin_date = date_util.date_str_from_day_delta(now_date, day_delta=-5)
    funds_flow_by_value(begin_date, now_date)
