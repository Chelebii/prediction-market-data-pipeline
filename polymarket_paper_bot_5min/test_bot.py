import sys
sys.path.insert(0, '.')
from polymarket_paper_bot import *

conn = db_connect()
db_init(conn)
print("DB OK")

for coin in COINS:
    sym = BINANCE_SYMBOLS[coin]
    m = get_binance_kline_momentum(sym, 60)
    p = get_binance_price(sym)
    print(f"{coin}: price={p}, momentum={m}")

slot = get_current_slot_ts()
sec = seconds_into_slot()
print(f"Current slot: {slot}, {sec}s into slot")

for coin in COINS:
    mk = get_5min_market(coin, slot)
    if mk:
        prices = get_market_prices(mk)
        closed = mk.get("closed", "")
        print(f"{coin} market: prices={prices}, closed={closed}")
    else:
        print(f"{coin} market: NOT FOUND")

print(f"Equity: {get_equity(conn)}")
