import datetime

def now_date_str():
    date = datetime.datetime.now()
    return '%04d-%02d-%02d' % (date.year, date.month, date.day)

def date_str_from_day_delta(date_str, format='%Y-%m-%d', day_delta=1):
    date = date_from_str(date_str, format)
    date = date + datetime.timedelta(days=day_delta)
    return '%04d-%02d-%02d' % (date.year, date.month, date.day)

def date_from_str(date_str, format='%Y-%m-%d'):
    date = datetime.datetime.strptime(date_str, format).date()
    return date