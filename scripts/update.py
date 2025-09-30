# scripts/update.py
import json
import pathlib
import sys
import time
from datetime import datetime, timezone
import requests

LLAMA_STABLES_URL = "https://stablecoins.llama.fi/stablecoins?includePrices=true"

# Що трекаємо
TOKENS = {"USDT", "USDC", "DAI", "PYUSD"}

# Мапа альясів → канон
CHAIN_ALIASES = {
    "ethereum": "Ethereum", "eth": "Ethereum",
    "arbitrum": "Arbitrum", "arbitrum one": "Arbitrum", "arb": "Arbitrum",
    "base": "Base",
    "optimism": "Optimism", "op": "Optimism", "op mainnet": "Optimism",
}

def canon_chain(name: str) -> str | None:
    if not name:
        return None
    key = str(name).strip().lower()
    return CHAIN_ALIASES.get(key, name)

def fetch_llama_stables():
    for attempt in range(3):
        try:
            r = requests.get(LLAMA_STABLES_URL, timeout=25)
            r.raise_for_status()
            data = r.json()
            if isinstance(data, dict) and "peggedAssets" in data:
                return data["peggedAssets"]
            raise ValueError("Unexpected response shape from DefiLlama")
        except Exception:
            if attempt == 2:
                raise
            time.sleep(1 + attempt)
    return []

def last_price_usd(asset: dict):
    prices = asset.get("prices") or []
    if not prices:
        return None
    try:
        return float(prices[-1].get("price"))
    except Exception:
        return None

def extract_circulating_usd(entry: dict):
    circ = entry.get("circulating")
    if isinstance(circ, dict):
        for k in ("peggedUSD", "peggedUsd", "usd", "value"):
            v = circ.get(k)
            if v is not None:
                try:
                    return float(v)
                except (TypeError, ValueError):
                    pass
    for k in ("peggedUSD", "peggedUsd", "usd", "value", "circulating", "circulatingPrevDay"):
        v = entry.get(k)
        if v is not None:
            try:
                return float(v)
            except (TypeError, ValueError):
                pass
    return None

def normalize(pegged_assets: list):
    """
    Формуємо рядки по всіх доступних мережах (без жорсткого фільтру),
    плюс додаємо TOTAL по кожному токену для надійності.
    """
    rows = []
    totals = {}  # symbol -> sum

    for asset in pegged_assets:
        symbol = (asset.get("symbol") or "").upper()
        if TOKENS and symbol not in TOKENS:
            continue

        price_usd = last_price_usd(asset)
        chain_circ = asset.get("chainCirculating") or []
        if not isinstance(chain_circ, list) or not chain_circ:
            continue

        for e in chain_circ:
            if not isinstance(e, dict):
                continue
            raw_chain = e.get("chain") or e.get("name")
            ch = canon_chain(raw_chain) or "Unknown"

            val = extract_circulating_usd(e)
            if val is None:
                continue

            rows.append({
                "symbol": symbol,
                "chain": ch,
                "circulatingUsd": round(val, 2),
                "priceUsd": price_usd
            })
            totals[symbol] = totals.get(symbol, 0.0) + float(val)

    # Додаємо підсумкові рядки TOTAL по кожному символу
    for sym, total in sorted(totals.items()):
        rows.append({
            "symbol": sym,
            "chain": "TOTAL",
            "circulatingUsd": round(total, 2),
            "priceUsd": None
        })

    rows.sort(key=lambda r: (r["symbol"], r["chain"]))
    return rows

def write_snapshot(rows: list) -> pathlib.Path:
    dt = datetime.now(timezone.utc)
    folder = pathlib.Path("data") / dt.strftime("%Y-%m-%d")
    folder.mkdir(parents=True, exist_ok=True)
    outpath = folder / f"{dt.strftime('%H%M%S')}.json"
    payload = {
        "ts": dt.isoformat(timespec="seconds"),
        "source": "DefiLlama Stablecoins",
        "filters": {"tokens": sorted(list(TOKENS))},
        "rows": rows
    }
    outpath.write_text(json.dumps(payload, indent=2))
    return outpath

if __name__ == "__main__":
    raw = fetch_llama_stables()
    norm = normalize(raw)
    out = write_snapshot(norm)
    print(f"[update.py] wrote file: {out} (rows={len(norm)})")
    sys.exit(0)
