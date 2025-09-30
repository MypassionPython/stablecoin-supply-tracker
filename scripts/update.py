import json
import pathlib
import sys
import time
from datetime import datetime, timezone

import requests

LLAMA_STABLES_URL = "https://stablecoins.llama.fi/stablecoins?includePrices=true"

TOKENS = {"USDT", "USDC", "DAI", "PYUSD"}
CHAINS = {"Ethereum", "Arbitrum", "Base", "Optimism"}  # можна розширити

def fetch_llama_stables():
    """Реальні дані з DefiLlama Stablecoins API (без ключів) з ретраями."""
    for attempt in range(3):
        try:
            r = requests.get(LLAMA_STABLES_URL, timeout=25)
            r.raise_for_status()
            data = r.json()
            if isinstance(data, dict) and "peggedAssets" in data:
                return data["peggedAssets"]
            raise ValueError("Unexpected response shape")
        except Exception as e:
            if attempt == 2:
                raise
            time.sleep(1 + attempt)
    return []

def last_price_usd(asset):
    """Витягуємо останню відому ціну з масиву prices (якщо є)."""
    prices = asset.get("prices") or []
    if not prices:
        return None
    # items зазвичай {"timestamp":..., "price":...}; беремо останній
    try:
        return float(prices[-1].get("price"))
    except Exception:
        return None

def normalize(pegged_assets):
    """
    Агрегуємо circulating USD по вибраних чейнах для вибраних токенів.
    Використовуємо asset['chainCirculating'] (масив об'єктів).
    """
    rows = []
    for asset in pegged_assets:
        symbol = (asset.get("symbol") or "").upper()
        if TOKENS and symbol not in TOKENS:
            continue

        price_usd = last_price_usd(asset)

        chain_circ = asset.get("chainCirculating") or []
        # УВАЖНО: chainCirculating — список ОБ’ЄКТІВ:
        # {"chain":"Ethereum","circulating":{"peggedUSD": ...}, ...}
        if isinstance(chain_circ, list) and chain_circ:
            for entry in chain_circ:
                if not isinstance(entry, dict):
                    # іноді трапляються рядки/сюрпризи — просто пропускаємо
                    continue
                chain_name = entry.get("chain") or entry.get("name")
                if CHAINS and chain_name not in CHAINS:
                    continue

                # Значення може лежати в різних ключах (перестрахуємось)
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
                    # деякі варіанти схеми
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
        else:
            # Якщо нема поканальної розбивки — пропускаємо без помилки.
            # (можна додати total по всіх чейнах, якщо потрібно)
            continue

    # відсортуємо для стабільності
    rows.sort(key=lambda r: (r["symbol"], r["chain"]))
