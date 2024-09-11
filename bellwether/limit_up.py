import util.date_util as date_util
import init.db_init as db_init
import init.tushare_init as tushare_init


def _limit_up_test(code, d):
    # d[0:2] close, preclose, isST
    c = code[3:6]
    if c[0:2] == '60' or c[0:2] == '00':
        lu = 0.05 if d[2] == 1 else 0.1
    elif c == '300' or c == '688':
        lu = 0.2
    else:
        lu = 0.3
    a = d[0]
    b = round(d[1] * (1 + lu), 2)
    if a == b:
        return True
    else:
        return False


def process(date=date_util.now_date_str(), days_before=30, frequency=1, test=_limit_up_test):

    def _limit_up_calc(code, data):
        # if len(data) < days_before:
        if len(data) < 1:
            return 0, 0, 0, 0, 0

        n_limit_up = 0
        c_limit_up = 0
        l_rise = 0
        c_stopped = False
        total_amount = 0
        for (i, d) in enumerate(data):
            rise = d[0] / d[1] - 1.0
            total_amount = total_amount + d[3]
            if i == 0:
                l_rise = rise
            if not test(code, d):
                c_stopped = True
                continue
            n_limit_up = n_limit_up + 1
            if not c_stopped:
                c_limit_up = c_limit_up + 1

        return n_limit_up, c_limit_up, l_rise, (data[0][1] / data[-1][1] - 1), data[0][3] / (total_amount / len(data))

    def _tx0(handler0):
        def _tx(handler):
            try:
                stock_info = handler.fetch_all(
                    'SELECT ts_code, name, industry FROM %s' % (db_init.DB_TABLE_STOCK_BASIC, ))

                handler0.drop_table(db_init.DB_TABLE_LIMIT_UP)
                handler0.create_table(db_init.DB_TABLE_LIMIT_UP, [
                    'code TEXT', 'name TEXT', 'n INTEGER', 'c INTEGER',
                    'l_rise REAL', 'r_rise REAL', 'climax REAL', 'industry TEXT', 'pe REAL'
                ])

                for si in stock_info:
                    code = tushare_init.baostock_code(si[0])
                    data = handler.fetch_all('SELECT close, preclose, isST, amount, peTTM FROM %s WHERE date <= ? '
                                             'AND code = ? AND amount <> \'\' ORDER BY date DESC LIMIT %d'
                                             % (db_init.DB_TABLE_STOCK_DAILY, days_before), [date, code])

                    nlu, clu, lr, rr, climax = _limit_up_calc(code, data)

                    if nlu >= frequency and clu > 0:
                        handler0.insert_into_table(db_init.DB_TABLE_LIMIT_UP, [
                            code,
                            si[1],
                            nlu, clu, round(lr, 2), round(rr, 2), round(climax, 2),
                            si[2],
                            data[0][4],
                        ])

            except Exception as e:
                print(e)

        db_init.execute(db_init.DB_PATH_MAIN, _tx)

    db_init.execute(db_init.DB_PATH_BELLWETHER, _tx0)

