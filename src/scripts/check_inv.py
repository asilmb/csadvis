import asyncio
from scrapper.steam_inventory import SteamInventoryClient

async def main():
    async with SteamInventoryClient() as c:
        items = await c.fetch("76561198044131975")
    cases = [i for i in items if "case" in (i.get("market_hash_name","") + i.get("item_type","")).lower()]
    print("Total items:", len(items))
    print("Cases found:", len(cases))
    for x in cases[:20]:
        print("  m=%d t=%d cnt=%d name=%s" % (x["marketable"], x["tradable"], x["count"], x["market_hash_name"]))

asyncio.run(main())
