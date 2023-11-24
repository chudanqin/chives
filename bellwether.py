import tushare_init
import db_init


def top_rise(number = 200):

    def _tx(handler):
        try:
            stock_info = handler.fetch_all(
                'SELECT ts_code, name, industry FROM %s' % (db_init.DB_TABLE_STOCK_BASIC))

            table_name = 'top_rise'
            handler.drop_table(table_name)
            handler.create_table(
                table_name, ['code TEXT', 'name TEXT', 'industry TEXT', 'rise REAL'])

            for si in stock_info:
                code = tushare_init.baostock_code(si[0])
                min_max = handler.fetch_first('SELECT cast(close as decimal), cast(amount as decimal) FROM %s WHERE code = ? and %s is not \'\' and %s > 0' % (
                    target, target, db_init.DB_TABLE_STOCK_DAILY, target, target), [code])
                curr = handler.fetch_first('SELECT cast(%s as decimal) FROM %s WHERE code = ? and %s is not \'\' ORDER BY date DESC limit 1' % (
                    target, db_init.DB_TABLE_STOCK_DAILY, target), [code])
                
                if min_max is None or len(min_max) < 2 or min_max[0] is None or min_max[0] is None:
                    continue
                if curr is None or len(curr) == 0:
                    continue

                min = min_max[0]
                max = min_max[1]
                curr = curr[0]
                if curr < 0:
                    continue

                p = (curr - min) / (max - min) if max > min else 1.0

                handler.insert_into_table(
                    table_name, [code, si[1], min, max, curr, p, si[2]])

        except Exception as e:
            print(e)

    db_init.execute(db_init.DB_PATH_MAIN, _tx)

