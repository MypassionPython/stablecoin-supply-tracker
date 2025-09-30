import json
import pathlib
import sys
import time
from datetime import datetime, timezone

import requests

# Публічне джерело з реальними даними
LLAMA_STABLES_URL = "https://stablecoins.llama.fi/stablecoins?includePrices=true"

# Що саме трекаємо
TOKENS = {"USDT", "USDC", "DAI", "PYUSD"}
CHAINS = {"Ethereum", "Arbitrum", "Base", "Optimism"}  # можна розширити

def fetch_llama_stables():
    """Реальні дані з DefiLlama Stablecoins API (без ключів)."""
    for attempt in range(3):
        try:
            r = requests.get(LLAMA_STABLES_URL, timeout=25)
            r.raise_for_status()
            data = r.json()
            # очікуємо структуру {"peggedAssets":[...]}
            if isinstance(data, dict) and "peggedAssets" in data:
                return data["peggedAssets"]
            raise ValueError("Unexpected response shape")
        except Exception:
            if attempt == 2:
                raise
            time.sleep(1 + attempt)
    return []

def normalize(pegged_assets):
    """
    Агрегуємо total circulating USD по вибраних чейнах для обраних токенів:
    результат: {ts, rows:[{symbol, chain, circulatingUsd, priceUsd}]}
    """
    rows = []
    # кожен елемент описує один стейбл; у ньому є масив "prices" та "chains"
    for asset in pegged_assets:
        symbol = (asset.get("symbol") or "").upper()
        if TOKENS and symbol not in TOKENS:
            continue

        # остання відома ціна (беремо перший елемент, якщо є)
        price_usd = None
        prices = asset.get("prices") or []
        if prices:
            # items вигляду {"timestamp":..., "price":...}
            try:
                price_usd = float(prices[-1].get("price"))
            except Exception:
                price_usd = None

        # ланцюги в "chains": [{"chain": "Ethereum", "circulating": ..., "circulatingPrevDay": ...}, ...]
        for ch in (asset.get("chains") or []):
            chain_name = ch.get("chain")
            if CHAINS and chain_name not in CHAINS:
                continue
            circ = ch.get("circulating", 0)
            # у відповіді часто в нативних одиницях → в API це вже в USD; якщо інакше — адаптуємо тут
            try:
                circulating_usd = float(circ)
            except Exception:
                continue
            rows.append({
                "symbol": symbol,
                "chain": chain_name,
                "circulatingUsd": round(circulating_usd, 2),
                "priceUsd": price_usd
            })

    # сортуємо за обсягом
    rows.sort(key=lambda r: (r["symbol"], r["chain"]))
    return rows

def write_snapshot(rows):
    dt = datetime.now(timezone.utc)
    folder = pathlib.Path("data") / dt.strftime("%Y-%m-%d")
    folder.mkdir(parents=True, exist_ok=True)
    outpath = folder / f"{dt.strftime('%H%M%S')}.json"
    payload = {
        "ts": dt.isoformat(timespec="seconds"),
        "source": "DefiLlama Stablecoins",
        "filters": {"tokens": sorted(list(TOKENS)), "chains": sorted(list(CHAINS))},
        "rows": rows
    }
    outpath.write_text(json.dumps(payload, indent=2))
    return outpath

if __name__ == "__main__":
    raw = fetch_llama_stables()
    normalized = normalize(raw)
    out = write_snapshot(normalized)
    print(f"[update.py] wrote file: {out}")
    sys.exit(0)
