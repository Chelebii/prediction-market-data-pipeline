import sys
sys.path.insert(0, '.')
from polymarket_paper_bot import *

conn = db_connect()
db_init(conn)
print("Running scan_once...")
scan_once(conn)

positions = load_open_positions(conn)
print(f"Open positions: {len(positions)}")
for p in positions:
    print(f"  {p[1]} {p[4]} @ {p[5]:.3f} (slot {p[3]})")

print(f"Equity: {get_equity(conn):.2f}")
