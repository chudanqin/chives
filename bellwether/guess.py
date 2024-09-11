import util.type_util as type_util
import init.db_init as db_init
import init.tushare_init as tushare_init


# 一段时间内，温和放量（与这段时间的平均值比较），波动较小（与这段时间的均价比较）
def _buy_and_sell(stock_info, data, short_days, long_days, t_days_sell=2):
    short_total = 0.0
    long_total = 0.0
    short_amount_list = []
    long_amount_list = []
    profit_total = 0.0
    i = 0

    for row in data:
        amount = type_util.parse_float(row[0])
        open = type_util.parse_float(row[1])
        close = type_util.parse_float(row[2])
        

        short_total += amount
        short_amount_list.append(amount)
        if len(short_amount_list) > short_days:
            short_total -= short_amount_list[0]
            short_amount_list.pop(0)

        long_total += amount
        long_amount_list.append(amount)
        if len(long_amount_list) > long_days:
            long_total -= long_amount_list[0]
            long_amount_list.pop(0)

            long_avg = long_total / long_days
            short_avg = short_total / short_days

            climax = short_avg / long_avg
            if climax > 1.2 and climax < 2.0:
                if len(data) > i + t_days_sell:
                    t_1_d = data[i + 1]
                    t_1_open = type_util.parse_float(t_1_d[1])
                    if _is_limit_up(stock_info[0], close, t_1_open, row[6]):
                        print('%s, %s-%s limit_up' % (t_1_d[5], stock_info[0], stock_info[1]))
                        break
                    t_n_d = data[i + t_days_sell]
                    t_n_open = type_util.parse_float(t_n_d[1])
                    t_n_close = type_util.parse_float(t_n_d[2])
                    profit = (t_n_open - t_1_open) / t_1_open
                    profit_total += profit
                    print('%s, (%s-%s), buy: %.2f, sell: %.2f, profit: %.2f' % (t_1_d[5], stock_info[0], stock_info[1], t_1_open, t_n_open, profit * 100))
                    break
        i += 1

    return profit_total


def process(begin_date, short_days=1, long_days=30):

    def _tx0(handler0):
        def _tx(handler):
            try:
                stock_info = handler.fetch_all(
                    'SELECT ts_code, name, industry FROM %s' % (db_init.DB_TABLE_STOCK_BASIC, ))

                # handler0.drop_table(db_init.DB_TABLE_CLIMAX_ANALYSIS)
                # handler0.create_table(db_init.DB_TABLE_CLIMAX_ANALYSIS, [
                #     'code TEXT', 'name TEXT', 'fast REAL', 'slow REAL', 'climax REAL', 'l_rise REAL', 'r_rise REAL',
                #     'pe REAL', 'pb REAL', 'industry TEXT'])

                profit_total = 0.0
                for si in stock_info:
                    code = tushare_init.baostock_code(si[0])
                    data = handler.fetch_all('SELECT amount, open, close, peTTM, pbMRQ, date, isST FROM %s WHERE date >= ? '
                                             'AND code = ? AND amount <> \'\' ORDER BY date'
                                             % (db_init.DB_TABLE_STOCK_DAILY),
                                             [begin_date, code])

                    p = _buy_and_sell(si, data, short_days, long_days)
                    profit_total += p
                print('total profit: %f' % profit_total)
            except Exception as e:
                print('%s: %s' % (__file__, e))

        db_init.execute(db_init.DB_PATH_MAIN, _tx)

    db_init.execute(db_init.DB_PATH_BELLWETHER, _tx0)


def _is_limit_up(code, prev, curr, st_flag):
    # d[0:2] close, preclose, isST
    c = code[0:3]
    if c[0:2] == '60' or c[0:2] == '00':
        lu = 0.05 if st_flag == 1 else 0.1
    elif c == '300' or c == '688':
        lu = 0.2
    else:
        lu = 0.3
    limit_up_value = round(prev * (1 + lu), 2)
    if curr == limit_up_value:
        return True
    else:
        return False
    