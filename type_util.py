

def _parse(value, to_type, default_value):
    if isinstance(value, to_type):
        return value
    if value is '':
        return default_value
    try:
        return to_type(value)
    except Exception as e:
        print(e)
        return default_value


def parse_float(value, default_value=0.0):
    return _parse(value, float, default_value)


def parse_int(value, default_value=0):
    return _parse(value, int, default_value)

