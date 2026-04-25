"""Check trade_advice values for top containers to diagnose flip no_profit."""
import sys
sys.path.insert(0, "/app/src")

from domain.connection import SessionLocal
from domain.models import DimContainer
from infra.cache_writer import _get_trade_advice_cached

with SessionLocal() as db:
    containers = db.query(DimContainer).filter(DimContainer.is_active == True).all()

try:
    advice = _get_trade_advice_cached()
except Exception as e:
    print(f"cache error: {e}")
    advice = {}

# Show top 20 by buy_target desc, focusing on cases with data
rows = []
for c in containers:
    a = advice.get(str(c.container_id), {})
    bt = a.get("buy_target", 0) or 0
    st = a.get("sell_target", 0) or 0
    cp = a.get("current_price", 0) or 0
    if bt > 0 and st > 0:
        net = st / 1.15 - 5 - bt
        rows.append((str(c.container_name), bt, st, cp, round(net, 1)))

rows.sort(key=lambda x: -x[1])
print(f"{'Name':<45} {'buy_t':>8} {'sell_t':>8} {'current':>8} {'net':>8}")
print("-" * 85)
for name, bt, st, cp, net in rows[:30]:
    flag = " *** NO PROFIT" if net <= 0 else (" <below_buy" if cp < bt and cp > 0 else "")
    print(f"{name:<45} {int(bt):>8} {int(st):>8} {int(cp):>8} {int(net):>8}{flag}")
