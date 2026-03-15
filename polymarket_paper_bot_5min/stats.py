import sqlite3
conn = sqlite3.connect('paper_trades.db')
total = conn.execute("SELECT COUNT(*) FROM paper_positions").fetchone()[0]
open_n = conn.execute("SELECT COUNT(*) FROM paper_positions WHERE status='OPEN'").fetchone()[0]
closed = conn.execute("SELECT COUNT(*) FROM paper_positions WHERE status='CLOSED'").fetchone()[0]
pnl = conn.execute("SELECT COALESCE(SUM(pnl_usd),0) FROM paper_positions WHERE status='CLOSED'").fetchone()[0]
wins = conn.execute("SELECT COUNT(*) FROM paper_positions WHERE status='CLOSED' AND pnl_usd>0").fetchone()[0]
losses = conn.execute("SELECT COUNT(*) FROM paper_positions WHERE status='CLOSED' AND pnl_usd<=0").fetchone()[0]
print(f"Total: {total}, Open: {open_n}, Closed: {closed}")
print(f"PnL: {pnl:.2f}$, W: {wins}, L: {losses}")
rows = conn.execute("SELECT coin, outcome, entry_price, exit_price, pnl_usd, close_reason FROM paper_positions WHERE status='CLOSED' ORDER BY closed_ts DESC LIMIT 5").fetchall()
for r in rows:
    sign = "+" if r[4] >= 0 else ""
    print(f"  {r[0]} {r[1]} @ {r[2]:.3f}->{r[3]:.3f} = {sign}{r[4]:.2f}$ ({r[5]})")
