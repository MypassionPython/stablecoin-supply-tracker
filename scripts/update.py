# scripts/update.py
import json
import pathlib
import sys
import time
from datetime import datetime, timezone

import requests

# Публічне джерело з реальними даними (без ключів)
LLAMA_STABLES_URL = "https://stablecoins.llama.fi/stablecoins?includePrices=true"

# Що саме трекаємо
TOKENS = {"USDT", "USDC", "DAI", "PYUSD"}
CHAINS = {"Ethereum", "Arbitrum", "Base", "Optimism"}  # можна розширити


def fetch_llama_stables():
    """Реальні дані з DefiLlama Stablecoins API з ретраями та базовою валідацією."""
    for attempt in range(3):
        try:
            r = requests.get(LLAMA_STABLES_URL, timeout=25)
            r.raise_for_status()
            data = r.json()
            if isinstance(data, dict) and "peggedAssets" in data:
                return data["peggedAssets"]
            raise ValueError("Unexpected response shape from DefiLlama")
        except Exception as e:
            if attempt == 2:
                # остання спроба — пробиваємо помилку, хай воркфлоу впаде і ти це побачиш
                raise
            time.sleep(1 + attempt)
    return []


def last_price_usd(asset: dict):
    """Витягуємо останню відому ціну з масиву prices (якщо є)."""
    prices = asset.get("prices") or []
    if not prices:
        return None
    try:
        return float(prices[-1].get("price"))
    except Exception:
        return None


def normalize(pegged_assets: list):
    """
    Агрегуємо circulating USD по вибраних чейнах для вибраних токенів.
    Основне поле — asset['chainCirculating']: список об'єктів:
      {"chain":"Ethereum","circulating":{"peggedUSD": ...}, ...}
    """
    rows = []
    for asset in pegged_assets:
        symbol = (asset.get("symbol") or "").upper()
        if TOKENS and symbol not in TOKENS:
            continue

        price_usd = last_price_usd(asset)
        chain_circ = asset.get("chainCirculating") or []

        if not isinstance(chain_circ, list) or not chain_circ:
            # немає поканальної розбивки — пропускаємо без помилки
            continue

        for entry in chain_circ:
            if not isinstance(entry, dict):
                # іноді можуть трапитись рядки або інші типи — скіпаємо
                continue

            chain_name = entry.get("chain") or entry.get("name")
            if CHAINS and chain_name not in CHAINS:
                continue

            # Значення може лежати у різних ключах — беремо найтиповіші
            val = None
            circ = entry.get("circulating")
            if isinstance(circ, dict):
                val = (
                    circ.get("peggedUSD")
                    or circ.get("peggedUsd")
                    or circ.get("usd")
                    or circ.get("value")
                )
            if val is None:
                # запасні варіанти
                val = entry.get("circulating") or entry.get("circulatingPrevDay")

            try:
                circulating_usd = float(val)
            except (TypeError, ValueError):
                continue

            rows.append({
                "symbol": symbol,
                "chain": chain_name,
                "circulatingUsd": round(circulating_usd, 2),
                "priceUsd": price_usd
            })

    # стабільне сортування для читабельності
    rows.sort(key=lambda r: (r["symbol"], r["chain"]))
    return rows


def write_snapshot(rows: list) -> pathlib.Path:
    """Пише JSON-снапшот у data/YYYY-MM-DD/HHMMSS.json (завжди нове ім'я → завжди diff)."""
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
    norm = normalize(raw)
    out = write_snapshot(norm)
    print(f"[update.py] wrote file: {out}")
    sys.exit(0)
