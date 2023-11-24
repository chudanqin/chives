import db_init
import tushare_init


def figure_band(target):
    def _tx0(handler0):

        def _tx(handler1):
            try:
                stock_info = handler1.fetch_all(
                    'SELECT ts_code, name, industry FROM %s' % (db_init.DB_TABLE_STOCK_BASIC))

                table_name = 'target_' + target
                handler0.drop_table(table_name)
                handler0.create_table(
                    table_name,
                    ['code TEXT', 'name TEXT', 'min REAL', 'max REAL', 'curr REAL', 'p REAL', 'industry TEXT'])

                for si in stock_info:
                    code = tushare_init.baostock_code(si[0])
                    min_max = handler1.fetch_first(
                        'SELECT min(cast(%s as decimal)), max(cast(%s as decimal)) FROM %s WHERE code = ? AND %s IS NOT \'\' and %s > 0' % (
                            target, target, db_init.DB_TABLE_STOCK_DAILY, target, target), [code])
                    curr = handler1.fetch_first(
                        'SELECT cast(%s as decimal) FROM %s WHERE code = ? and %s is not \'\' ORDER BY date DESC limit 1' % (
                            target, db_init.DB_TABLE_STOCK_DAILY, target), [code])

                    if min_max is None or len(min_max) < 2 or min_max[0] is None or min_max[0] is None:
                        continue
                    if curr is None or len(curr) == 0:
                        continue

                    min_price = min_max[0]
                    max_price = min_max[1]
                    curr = curr[0]
                    if curr < 0:
                        continue

                    p = (curr - min_price) / (max_price - min_price) if max_price > min_price else 1.0

                    handler0.insert_into_table(
                        table_name, [code, si[1], min_price, max_price, curr, round(p, 3), si[2]])

            except Exception as e:
                print(e)

        db_init.execute(db_init.DB_PATH_MAIN, _tx)

    db_init.execute(db_init.DB_PATH_BELLWETHER, _tx0)


def test():
    figure_band('peTTM')
    figure_band('pbMRQ')
