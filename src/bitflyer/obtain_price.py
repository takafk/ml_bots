import ccxt
from pprint import pprint

bitflyer = ccxt.bitflyer()
ticker = bitflyer.fetch_ticker('BTC/JPY', params = { "product_code" : "FX_BTC_JPY" })
pprint(ticker)