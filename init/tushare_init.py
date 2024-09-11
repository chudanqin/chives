import tushare as ts

_TOKEN = '512282ee9ad569746b6b267c148a0e892ec20308cd6d051168dfd4fa'
API = ts.pro_api(_TOKEN)


def baostock_code(ts_code):
    symbols = ts_code.split('.')
    if len(symbols) == 2:
        return '%s.%s' % (symbols[1].lower(), symbols[0])
    else:
        return ts_code

