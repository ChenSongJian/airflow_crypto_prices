import ccxt


def get_ccxt_ex(exchange):
    if exchange == 'binance':
        ex = ccxt.binance()
    elif exchange == 'bybit':
        ex = ccxt.bybit()
    elif exchange == 'okx':
        ex = ccxt.okx()
    elif exchange == 'kraken':
        ex = ccxt.bybit()
    elif exchange == 'mexc':
        ex = ccxt.okx()
    else:
        raise Exception(f'Exchange {exchange} not supported!')
    ex.enableRateLimit = True
    return ex