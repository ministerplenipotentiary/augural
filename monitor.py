import os
import re
import sys
import json
import time
import socket
import threading
import requests
import feedparser
from difflib import SequenceMatcher
from datetime import datetime, timezone, timedelta
from pathlib import Path
from dotenv import load_dotenv
import anthropic

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

load_dotenv()

ANTHROPIC_API_KEY  = os.getenv("ANTHROPIC_API_KEY")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID")
ETHERSCAN_API_KEY  = os.getenv("ETHERSCAN_API_KEY")

WATCHLIST = ["HYPE", "BTC", "ETH", "SOL"]

TOKEN_TERMS = {
    "HYPE": ["hype", "hyperliquid"],
    "BTC":  ["btc", "bitcoin"],
    "ETH":  ["eth", "ethereum", "ether"],
    "SOL":  ["sol", "solana"],
}

# Whole-word regex for macro/geopolitical terms — avoids substrings like
# "war" in "warns", "oil" in "spoiled", "fed" in "confirmed", etc.
_MACRO_WORDS = [
    "war", "oil", "fed", "iran", "china", "russia",
    "hormuz", "opec", "federal reserve", "inflation",
    "sanctions", "ceasefire", "nuclear", "taiwan", "trump", "tariffs",
]
_MACRO_RE = re.compile(
    r'\b(?:' + '|'.join(re.escape(w) for w in _MACRO_WORDS) + r')\b',
    re.IGNORECASE,
)

COINGECKO_IDS = {
    "BTC":  "bitcoin",
    "ETH":  "ethereum",
    "SOL":  "solana",
    "HYPE": "hyperliquid",
}


RSS_FEEDS = [
    {"name": "CoinDesk",        "url": "https://www.coindesk.com/arc/outboundfeeds/rss/"},
    {"name": "CoinTelegraph",   "url": "https://cointelegraph.com/rss"},
    {"name": "Decrypt",         "url": "https://decrypt.co/feed"},
    {"name": "The Block",       "url": "https://www.theblock.co/rss.xml"},
    {"name": "Blockworks",      "url": "https://blockworks.co/feed"},
    {"name": "IRNA",            "url": "https://en.irna.ir/rss"},
    {"name": "Tasnim",          "url": "https://www.tasnimnews.com/en/rss/feed/0"},
    {"name": "Mehr News",       "url": "https://en.mehrnews.com/rss"},
    {"name": "Iran International", "url": "https://www.iranintl.com/en/rss"},
    {"name": "Middle East Eye", "url": "https://www.middleeasteye.net/rss"},
]

# Sources treated as HIGH by default when they match any MACRO keyword —
# official government wires where publication itself signals significance
AUTO_HIGH_SOURCES = {"IRNA", "Tasnim"}

POLL_INTERVAL          = 300
MACRO_INTERVAL         = 14400
BRIEF_HOUR             = 8
WHALE_MIN_USD          = 1_000_000   # BTC/ETH/SOL threshold
HYPE_TRADE_MIN_USD     = 500_000     # HYPE DEX threshold
ALERT_RATE_LIMIT       = 3           # max individual Telegram alerts per window
ALERT_WINDOW_SECS      = 600         # 10-minute rate-limit window
HEADLINE_SIM_THRESH    = 0.78        # SequenceMatcher ratio to treat as duplicate
FUNDING_INTERVAL       = 900         # 15 minutes
FUNDING_ALERT_PCT      = 0.05        # ±0.05% per 8h = alert
FUNDING_EXTREME_PCT    = 0.10        # ±0.10% per 8h = extreme alert
FUNDING_COINS          = ["HYPE", "BTC", "ETH", "SOL"]

# Hyperliquid coin names differ from our labels
HL_COIN_MAP = {"HYPE": "HYPE", "BTC": "BTC", "ETH": "ETH", "SOL": "SOL"}
SEEN_IDS_FILE       = "seen_ids.json"
WHALE_SEEN_FILE     = "seen_whales.json"
FILTERED_LOG        = "filtered_log.txt"
FOREXFACTORY_URL    = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"
LOCK_PORT           = 47832


# ── single-instance lock ──────────────────────────────────────

_lock_socket = None

def acquire_single_instance_lock():
    global _lock_socket
    for attempt in range(2):
        _lock_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        _lock_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 0)
        try:
            _lock_socket.bind(("127.0.0.1", LOCK_PORT))
            return  # success
        except OSError:
            _lock_socket.close()
            if attempt == 0:
                # Check if the port holder is actually responding; if not it's stale
                probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                probe.settimeout(1)
                try:
                    probe.connect(("127.0.0.1", LOCK_PORT))
                    probe.close()
                    # Something real is listening — genuinely already running
                    print("[error] Monitor is already running. Kill existing process first.")
                    sys.exit(1)
                except (ConnectionRefusedError, OSError):
                    probe.close()
                    # Port bound but not listening — stale lock from a crashed process
                    print("[warn] Stale lock detected, clearing and retrying...")
                    time.sleep(1)
            else:
                print("[error] Could not acquire startup lock. Kill any existing monitor process.")
                sys.exit(1)


# ── state ─────────────────────────────────────────────────────

class State:
    def __init__(self):
        self._lock               = threading.Lock()
        self.last_alert_time     = None
        self.filtered_today      = 0
        self.filtered_date       = datetime.now().date()
        self.last_macro_check    = 0.0
        self.last_brief_date     = None
        self.whale_alerts_today  = 0
        self.whale_alert_date    = datetime.now().date()
        self.last_eth_block      = 0
        self.last_funding_check  = 0.0
        self.last_funding_rates  = {}   # token -> rate pct at last alert
        self.recent_alert_times  = []   # timestamps of Telegram news alerts sent
        self.recent_headlines    = []   # (timestamp, title_lower) for dedup

    def increment_filtered(self):
        with self._lock:
            today = datetime.now().date()
            if today != self.filtered_date:
                self.filtered_today = 0
                self.filtered_date  = today
            self.filtered_today += 1

    def increment_whale(self):
        with self._lock:
            today = datetime.now().date()
            if today != self.whale_alert_date:
                self.whale_alerts_today = 0
                self.whale_alert_date   = today
            self.whale_alerts_today += 1

    def set_last_alert(self):
        with self._lock:
            self.last_alert_time = datetime.now()

    def get_stats(self):
        with self._lock:
            return {
                "last_alert":        self.last_alert_time,
                "filtered_today":    self.filtered_today,
                "whale_alerts_today": self.whale_alerts_today,
            }


# ── persistence ───────────────────────────────────────────────

def load_seen_ids():
    if Path(SEEN_IDS_FILE).exists():
        with open(SEEN_IDS_FILE) as f:
            return set(json.load(f))
    return set()


def save_seen_ids(seen_ids):
    with open(SEEN_IDS_FILE, "w") as f:
        json.dump(list(seen_ids), f)


def load_seen_whales():
    if Path(WHALE_SEEN_FILE).exists():
        with open(WHALE_SEEN_FILE) as f:
            return set(json.load(f))
    return set()


def save_seen_whales(seen):
    # Keep only the most recent 2000 IDs to prevent unbounded growth
    with open(WHALE_SEEN_FILE, "w") as f:
        json.dump(list(seen)[-2000:], f)


def log_filtered(article, significance):
    ts   = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] [{significance}] [{','.join(article['tokens'])}] {article['title']}  (via {article['source']})\n"
    with open(FILTERED_LOG, "a", encoding="utf-8") as f:
        f.write(line)


# ── macro calendar ────────────────────────────────────────────

def fetch_macro_events():
    try:
        resp = requests.get(
            FOREXFACTORY_URL,
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=10,
        )
        resp.raise_for_status()
        events = resp.json()
    except Exception as e:
        print(f"  [warn] Macro fetch failed: {e}")
        return []

    now    = datetime.now(timezone.utc)
    cutoff = now + timedelta(hours=24)
    upcoming = []
    for ev in events:
        if ev.get("impact") != "High" or ev.get("country") != "USD":
            continue
        date_str = ev.get("date", "")
        if not date_str:
            continue
        try:
            ev_time = datetime.fromisoformat(date_str)
            if ev_time.tzinfo is None:
                ev_time = ev_time.replace(tzinfo=timezone.utc)
            if now <= ev_time <= cutoff:
                upcoming.append({
                    "title":    ev.get("title", ""),
                    "time":     ev_time,
                    "forecast": ev.get("forecast", ""),
                    "previous": ev.get("previous", ""),
                })
        except Exception:
            continue
    return sorted(upcoming, key=lambda x: x["time"])


def send_macro_digest(events):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID or not events:
        return
    lines = ["<b>📅 MACRO CALENDAR — High Impact USD Events (next 24h)</b>\n"]
    for ev in events:
        local_time = ev["time"].astimezone().strftime("%H:%M")
        detail = ""
        if ev["forecast"]:
            detail += f" | Forecast: {ev['forecast']}"
        if ev["previous"]:
            detail += f" | Prev: {ev['previous']}"
        lines.append(f"🔴 <b>{local_time}</b>  {ev['title']}{detail}")
    lines.append("\n<i>High impact events can cause sudden crypto volatility — manage leverage.</i>")
    _tg_send(TELEGRAM_CHAT_ID, "\n".join(lines))


# ── prices ────────────────────────────────────────────────────

def fetch_prices(tokens=None):
    if tokens is None:
        tokens = WATCHLIST
    ids = ",".join(COINGECKO_IDS[t] for t in tokens if t in COINGECKO_IDS)
    if not ids:
        return {}
    try:
        resp = requests.get(
            "https://api.coingecko.com/api/v3/coins/markets",
            params={"vs_currency": "usd", "ids": ids, "price_change_percentage": "1h"},
            timeout=10,
        )
        resp.raise_for_status()
        id_to_token = {v: k for k, v in COINGECKO_IDS.items()}
        return {
            id_to_token[c["id"]]: {
                "price":     c.get("current_price", 0),
                "change_1h": c.get("price_change_percentage_1h_in_currency") or 0,
            }
            for c in resp.json() if c["id"] in id_to_token
        }
    except Exception as e:
        print(f"  [warn] Price fetch failed: {e}")
        return {}


def format_price_line(token, data):
    price  = data.get("price", 0)
    change = data.get("change_1h", 0)
    sign   = "+" if change >= 0 else ""
    if price >= 1000:
        price_str = f"${price:,.0f}"
    elif price >= 1:
        price_str = f"${price:,.2f}"
    else:
        price_str = f"${price:.4f}"
    return f"{token} {price_str} ({sign}{change:.1f}% 1h)"


# ── morning brief ─────────────────────────────────────────────

def generate_brief_context(prices, macro_events, funding_rates):
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    price_summary = ", ".join(
        format_price_line(t, prices[t]) for t in WATCHLIST if t in prices
    )
    macro_summary = (
        ", ".join(e["title"] for e in macro_events)
        if macro_events else "no major macro events today"
    )
    funding_summary = ", ".join(
        f"{t} {'+' if r >= 0 else ''}{r:.4f}%/8h" for t, r in funding_rates.items()
    ) if funding_rates else "unavailable"
    msg = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=180,
        messages=[{"role": "user", "content": (
            f"Prices: {price_summary}\n"
            f"Funding rates (per 8h): {funding_summary}\n"
            f"Today's macro events: {macro_summary}\n\n"
            "Write exactly 3 sentences of morning market context for a leveraged altcoin trader. "
            "Cover: overall market tone, key funding rate risk (overcrowded longs/shorts), "
            "and one specific actionable observation. Be direct — no fluff."
        )}],
    )
    return msg.content[0].text.strip()


def send_morning_brief(state):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    prices        = fetch_prices(WATCHLIST)
    macro_events  = fetch_macro_events()
    funding_rates = fetch_funding_rates()
    context       = generate_brief_context(prices, macro_events, funding_rates)
    price_lines   = [format_price_line(t, prices[t]) for t in WATCHLIST if t in prices]
    macro_lines   = (
        [f"🔴 {ev['time'].astimezone().strftime('%H:%M')} — {ev['title']}" for ev in macro_events]
        if macro_events else ["No high-impact USD events today"]
    )
    funding_lines = (
        [format_funding_line(t, funding_rates[t]) for t in FUNDING_COINS if t in funding_rates]
        if funding_rates else ["Funding data unavailable"]
    )
    text = (
        f"☀️ <b>MORNING BRIEF — {datetime.now().strftime('%A %d %b')}</b>\n\n"
        f"<b>Prices</b>\n" + "\n".join(price_lines) +
        f"\n\n<b>Funding Rates</b>\n" + "\n".join(funding_lines) +
        f"\n\n<b>Today's Macro</b>\n" + "\n".join(macro_lines) +
        f"\n\n<b>Market Context</b>\n{context}"
    )
    _tg_send(TELEGRAM_CHAT_ID, text)
    state.last_brief_date = datetime.now().date()
    if funding_rates:
        state.last_funding_rates = funding_rates
    print("  Morning brief sent.")


# ── news fetching ─────────────────────────────────────────────

def tokens_in_text(*texts):
    combined = " ".join(t.lower() for t in texts if t)
    matched = [tok for tok, terms in TOKEN_TERMS.items() if any(t in combined for t in terms)]
    if _MACRO_RE.search(combined):
        matched.append("MACRO")
    return matched


def fetch_all_articles():
    seen = {}
    for feed in RSS_FEEDS:
        try:
            parsed = feedparser.parse(feed["url"])
            for entry in parsed.entries:
                title   = entry.get("title", "")
                summary = entry.get("summary", "")
                link    = entry.get("link", "")
                if not link:
                    continue
                matched = tokens_in_text(title, summary)
                if matched:
                    seen[link] = {"id": link, "title": title, "summary": summary, "source": feed["name"], "tokens": matched}
        except Exception as e:
            print(f"  [warn] RSS fetch failed ({feed['name']}): {e}")
    return list(seen.values())


# ── whale monitoring ──────────────────────────────────────────

def _short_addr(addr):
    """Shorten a blockchain address for display."""
    if not addr or len(addr) < 10:
        return addr or "Unknown"
    return f"{addr[:6]}…{addr[-4:]}"


def fetch_btc_whales(seen_whale_ids, btc_price):
    """Monitor BTC mempool for large unconfirmed transactions via Blockchain.com."""
    if btc_price <= 0:
        return []
    try:
        resp = requests.get(
            "https://blockchain.info/unconfirmed-transactions?format=json",
            timeout=15,
        )
        resp.raise_for_status()
        txs = resp.json().get("txs", [])
        events = []
        for tx in txs:
            tx_id = tx.get("hash", "")
            if not tx_id or tx_id in seen_whale_ids:
                continue
            total_sat  = sum(o.get("value", 0) for o in tx.get("out", []))
            btc_amount = total_sat / 1e8
            usd_value  = btc_amount * btc_price
            if usd_value < WHALE_MIN_USD:
                continue
            from_addrs = [inp.get("prev_out", {}).get("addr", "") for inp in tx.get("inputs", [])]
            to_addrs   = [o.get("addr", "") for o in tx.get("out", []) if o.get("addr")]
            events.append({
                "id":         tx_id,
                "token":      "BTC",
                "amount_usd": usd_value,
                "amount":     btc_amount,
                "from_label": _short_addr(from_addrs[0]) if from_addrs else "Unknown",
                "to_label":   _short_addr(to_addrs[0])   if to_addrs   else "Unknown",
                "tx_type":    "transfer",
            })
        return events
    except Exception as e:
        print(f"  [warn] BTC blockchain fetch failed: {e}")
        return []


def fetch_eth_whales(seen_whale_ids, eth_price, state):
    """Monitor ETH for large transfers in the latest block via Etherscan V2 API."""
    if not ETHERSCAN_API_KEY or eth_price <= 0:
        return []
    try:
        resp = requests.get(
            "https://api.etherscan.io/v2/api",
            params={"chainid": 1, "module": "proxy", "action": "eth_blockNumber", "apikey": ETHERSCAN_API_KEY},
            timeout=10,
        )
        resp.raise_for_status()
        latest_block = int(resp.json().get("result", "0x0"), 16)
        if state.last_eth_block == 0:
            state.last_eth_block = latest_block - 1
        if latest_block <= state.last_eth_block:
            return []

        resp2 = requests.get(
            "https://api.etherscan.io/v2/api",
            params={
                "chainid": 1,
                "module":  "proxy",
                "action":  "eth_getBlockByNumber",
                "tag":     hex(latest_block),
                "boolean": "true",
                "apikey":  ETHERSCAN_API_KEY,
            },
            timeout=15,
        )
        resp2.raise_for_status()
        block_data   = resp2.json().get("result") or {}
        transactions = block_data.get("transactions", [])
        state.last_eth_block = latest_block

        events = []
        for tx in transactions:
            tx_id = tx.get("hash", "")
            if not tx_id or tx_id in seen_whale_ids:
                continue
            try:
                eth_amount = int(tx.get("value", "0x0"), 16) / 1e18
            except (ValueError, TypeError):
                continue
            usd_value = eth_amount * eth_price
            if usd_value < WHALE_MIN_USD:
                continue
            to_raw = tx.get("to") or ""
            events.append({
                "id":         tx_id,
                "token":      "ETH",
                "amount_usd": usd_value,
                "amount":     eth_amount,
                "from_label": _short_addr(tx.get("from", "")),
                "to_label":   _short_addr(to_raw) if to_raw else "Contract",
                "tx_type":    "transfer",
            })
        return events
    except Exception as e:
        print(f"  [warn] ETH Etherscan fetch failed: {e}")
        return []


def fetch_sol_whales(seen_whale_ids, sol_price):
    """Monitor SOL for large transfers via Solana mainnet RPC (getBlock)."""
    if sol_price <= 0:
        return []
    try:
        slot_resp = requests.post(
            "https://api.mainnet-beta.solana.com",
            json={"jsonrpc": "2.0", "id": 1, "method": "getSlot", "params": []},
            timeout=10,
        )
        slot_resp.raise_for_status()
        slot = slot_resp.json().get("result", 0) - 2  # go back 2 to ensure finalized

        block_resp = requests.post(
            "https://api.mainnet-beta.solana.com",
            json={
                "jsonrpc": "2.0", "id": 2,
                "method":  "getBlock",
                "params":  [slot, {
                    "encoding":                  "jsonParsed",
                    "transactionDetails":        "full",
                    "maxSupportedTransactionVersion": 0,
                    "rewards":                   False,
                }],
            },
            timeout=20,
        )
        block_resp.raise_for_status()
        txs = (block_resp.json().get("result") or {}).get("transactions", [])

        events = []
        for tx in txs:
            meta = tx.get("meta") or {}
            if meta.get("err"):
                continue
            pre  = meta.get("preBalances", [])
            post = meta.get("postBalances", [])
            if not pre or not post:
                continue
            # Largest single-account lamport decrease = amount sent
            max_decrease = max((p - q for p, q in zip(pre, post) if p > q), default=0)
            sol_amount = max_decrease / 1e9
            usd_value  = sol_amount * sol_price
            if usd_value < WHALE_MIN_USD:
                continue
            sig = (tx.get("transaction", {})
                     .get("signatures", [""])[0])
            if not sig or sig in seen_whale_ids:
                continue
            keys = (tx.get("transaction", {})
                      .get("message", {})
                      .get("accountKeys", []))
            from_key = keys[0].get("pubkey", "") if keys else ""
            to_key   = keys[1].get("pubkey", "") if len(keys) > 1 else ""
            events.append({
                "id":         sig,
                "token":      "SOL",
                "amount_usd": usd_value,
                "amount":     sol_amount,
                "from_label": _short_addr(from_key),
                "to_label":   _short_addr(to_key) if to_key else "Unknown",
                "tx_type":    "transfer",
            })
        return events
    except Exception as e:
        print(f"  [warn] SOL RPC fetch failed: {e}")
        return []


def fetch_hype_large_trades(seen_whale_ids):
    """Return large trades/liquidations on Hyperliquid DEX for HYPE."""
    try:
        resp = requests.post(
            "https://api.hyperliquid.xyz/info",
            json={"type": "recentTrades", "coin": "HYPE"},
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
        resp.raise_for_status()
        trades = resp.json()
        events = []
        for trade in trades:
            trade_id = trade.get("hash") or f"{trade.get('time',0)}-{trade.get('px',0)}-{trade.get('sz',0)}"
            if trade_id in seen_whale_ids:
                continue
            try:
                px  = float(trade.get("px", 0))
                sz  = float(trade.get("sz", 0))
                usd = px * sz
            except (ValueError, TypeError):
                continue
            if usd >= HYPE_TRADE_MIN_USD:
                events.append({
                    "id":         trade_id,
                    "token":      "HYPE",
                    "amount_usd": usd,
                    "amount":     sz,
                    "price":      px,
                    "side":       "Buy" if trade.get("side") == "B" else "Sell",
                    "time":       trade.get("time", 0),
                })
        return events
    except Exception as e:
        print(f"  [warn] Hyperliquid trades fetch failed: {e}")
        return []


def format_whale_move(event):
    """One-line description of the whale event."""
    if event["token"] == "HYPE":
        return (
            f"${event['amount_usd']:,.0f} {event['side']} on Hyperliquid "
            f"({event['amount']:,.0f} HYPE @ ${event['price']:.2f})"
        )
    return (
        f"${event['amount_usd']:,.0f} moved: "
        f"{event['from_label']} -> {event['to_label']}"
    )


# ── funding rates ────────────────────────────────────────────

def fetch_funding_rates():
    """Return {token: funding_rate_pct} for FUNDING_COINS via Hyperliquid public API.
    Hyperliquid reports funding as a decimal per 8h (e.g. 0.0001 = 0.01%)."""
    try:
        resp = requests.post(
            "https://api.hyperliquid.xyz/info",
            json={"type": "metaAndAssetCtxs"},
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
        resp.raise_for_status()
        meta, ctxs = resp.json()
        coin_names = [asset["name"] for asset in meta.get("universe", [])]
        rates = {}
        for name, ctx in zip(coin_names, ctxs):
            token = next((k for k, v in HL_COIN_MAP.items() if v == name), None)
            if token and token in FUNDING_COINS:
                try:
                    rates[token] = float(ctx.get("funding", 0)) * 100  # decimal -> pct
                except (TypeError, ValueError):
                    pass
        return rates
    except Exception as e:
        print(f"  [warn] Funding rate fetch failed: {e}")
        return {}


def _funding_label(rate_pct):
    abs_r = abs(rate_pct)
    if abs_r >= FUNDING_EXTREME_PCT:
        bias = "EXTREME LONG BIAS" if rate_pct > 0 else "EXTREME SHORT BIAS"
    elif abs_r >= FUNDING_ALERT_PCT:
        bias = "HIGH LONG BIAS" if rate_pct > 0 else "HIGH SHORT BIAS"
    else:
        bias = "NEUTRAL"
    return bias


def analyse_funding_rate(token, rate_pct):
    """Claude analysis of a notable funding rate."""
    client  = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    label   = _funding_label(rate_pct)
    sign    = "+" if rate_pct >= 0 else ""
    context = (
        f"{token} perpetual funding rate is {sign}{rate_pct:.4f}% per 8h ({label}). "
        f"{'Positive funding means longs pay shorts — longs are overcrowded.' if rate_pct > 0 else 'Negative funding means shorts pay longs — shorts are overcrowded.'}"
    )
    msg = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=120,
        messages=[{"role": "user", "content": (
            f"Funding rate data for a leveraged crypto trader:\n{context}\n\n"
            "Respond in exactly this format:\n"
            "Signal: [BEARISH / BULLISH / NEUTRAL]\n"
            "1-4H IMPACT: One sentence on likely price effect.\n"
            "WATCH: One specific thing to monitor."
        )}],
    )
    return msg.content[0].text.strip()


def print_funding_alert(token, rate_pct, analysis):
    ts    = datetime.now().strftime("%H:%M:%S")
    sign  = "+" if rate_pct >= 0 else ""
    label = _funding_label(rate_pct)
    print("\n" + "=" * 62)
    print(f"  {ts}  📊 FUNDING ALERT  [{token}]")
    print(f"  Rate: {sign}{rate_pct:.4f}% per 8h  ({label})")
    print()
    for line in analysis.strip().splitlines():
        print(f"  {line}")
    print("=" * 62)


def send_telegram_funding_alert(token, rate_pct, analysis):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    sign  = "+" if rate_pct >= 0 else ""
    label = _funding_label(rate_pct)
    parsed = {}
    for line in analysis.strip().splitlines():
        if ":" in line:
            k, _, v = line.partition(":")
            parsed[k.strip()] = v.strip()
    signal = _html_escape(parsed.get("Signal", ""))
    impact = _html_escape(parsed.get("1-4H IMPACT", ""))
    watch  = _html_escape(parsed.get("WATCH", ""))
    text = (
        f"📊 <b>FUNDING ALERT</b> [{token}]\n"
        f"Rate: {sign}{rate_pct:.4f}% per 8h ({label})\n"
        f"Signal: {signal}\n"
        f"1-4H IMPACT: {impact}\n"
        f"WATCH: {watch}"
    )
    _tg_send(TELEGRAM_CHAT_ID, text)


def format_funding_line(token, rate_pct):
    sign = "+" if rate_pct >= 0 else ""
    flag = ""
    if abs(rate_pct) >= FUNDING_EXTREME_PCT:
        flag = " ⚠️"
    elif abs(rate_pct) >= FUNDING_ALERT_PCT:
        flag = " ⚡"
    return f"{token}: {sign}{rate_pct:.4f}%/8h{flag}"


# ── alert dedup & rate limiting ──────────────────────────────

def _is_duplicate_headline(title, recent_headlines):
    """True if title is too similar to a headline sent in the last hour."""
    t       = title.lower()
    cutoff  = time.time() - 3600
    for ts, prev in recent_headlines:
        if ts < cutoff:
            continue
        if SequenceMatcher(None, t, prev).ratio() >= HEADLINE_SIM_THRESH:
            return True
    return False


def _tg_send_batch(articles_with_context):
    """Send a single Telegram message listing multiple HIGH articles."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    lines = [f"📰 <b>ALERT BATCH</b> — {len(articles_with_context)} stories (rate-limited)\n"]
    for article, _, price_lines in articles_with_context:
        token_str = " ".join(f"[{t}]" for t in article["tokens"])
        price_str = "  " + " | ".join(price_lines) if price_lines else ""
        lines.append(f"{token_str} <b>{_html_escape(article['title'])}</b> — <i>{article['source']}</i>{price_str}")
    _tg_send(TELEGRAM_CHAT_ID, "\n".join(lines))


# ── analysis ─────────────────────────────────────────────────

def quick_significance(title, tokens, source):
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    msg    = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=10,
        messages=[{"role": "user", "content": (
            f"Crypto news headline: \"{title}\"\n"
            f"Tokens: {', '.join(tokens)} | Source: {source}\n\n"
            "Rate significance for a SHORT-TERM LEVERAGED altcoin trader (1-4h horizon).\n\n"
            "Rate HIGH if ANY of these apply:\n"
            "- Geopolitical or macro event directly moving crypto (wars, sanctions, trade deals)\n"
            "- ANY mention of: Iran, Strait of Hormuz, oil supply, OPEC, Federal Reserve, "
            "inflation data, Russia, China, Taiwan, Trump, tariffs, nuclear, war, ceasefire — "
            "these are ALWAYS HIGH if they appear in the headline\n"
            "- Price milestone broken or reclaimed (e.g. 'BTC retakes $X')\n"
            "- On-chain signals: accumulation data, whale moves, exchange inflows/outflows\n"
            "- ANY news mentioning HYPE or Hyperliquid specifically\n"
            "- Regulatory action, exchange hack, protocol exploit\n"
            "- Large fund flows, ETF data, liquidation events\n"
            "- Derivatives data: funding rates, open interest, options expiry\n\n"
            "Rate MEDIUM for general market commentary, minor updates, analyst opinions.\n"
            "Rate LOW for opinion pieces, unrelated news, historical retrospectives.\n\n"
            "Reply with one word only: HIGH, MEDIUM, or LOW."
        )}],
    )
    word = msg.content[0].text.strip().upper()
    if "HIGH"   in word: return "HIGH"
    if "MEDIUM" in word: return "MEDIUM"
    return "LOW"


def full_analysis(title, tokens, source):
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    msg    = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=350,
        messages=[{"role": "user", "content": (
            "You are analysing breaking crypto news for a leveraged altcoin trader "
            "trading on short timeframes (1-4 hour horizon). Focus entirely on "
            "immediate price action, not long-term fundamentals.\n\n"
            f"Headline: {title}\n"
            f"Tokens: {', '.join(tokens)}\n"
            f"Source: {source}\n\n"
            "Respond in exactly this format:\n"
            "SIGNAL: [BULLISH / BEARISH / NEUTRAL]\n"
            "SIGNIFICANCE: HIGH\n"
            "1-4H IMPACT: One sentence — likely price move in next 1-4 hours and direction.\n"
            "LEVERAGE RISK: One sentence — specific risk to leveraged positions right now.\n"
            "WATCH: One price level, event, or data point to monitor in the next hour."
        )}],
    )
    return msg.content[0].text


def analyse_whale_event(event):
    """Claude analysis tailored to on-chain whale movements."""
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    move   = format_whale_move(event)

    if event["token"] == "HYPE":
        context = (
            f"Large {event['side']} trade on Hyperliquid perps DEX: {move}. "
            f"This may indicate a large trader opening/closing a leveraged position or a liquidation."
        )
    else:
        context = (
            f"On-chain whale transaction: {move}. "
            f"Exchange -> Unknown usually means withdrawal (accumulation, bullish). "
            f"Unknown -> Exchange usually means deposit (selling pressure, bearish). "
            f"Exchange -> Exchange may indicate an OTC transfer."
        )

    msg = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=200,
        messages=[{"role": "user", "content": (
            f"Whale event context for a leveraged crypto trader:\n{context}\n\n"
            "Respond in exactly this format:\n"
            "SIGNAL: [BULLISH / BEARISH / NEUTRAL]\n"
            "Suggests: One short phrase describing what this movement likely means\n"
            "1-4H IMPACT: One sentence on likely price effect in the next 1-4 hours."
        )}],
    )
    return msg.content[0].text.strip()


# ── output & telegram ─────────────────────────────────────────

def _html_escape(text):
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _tg_send(chat_id, text):
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": chat_id, "text": text, "parse_mode": "HTML"},
            timeout=10,
        )
    except Exception as e:
        print(f"  [warn] Telegram send failed: {e}")


def print_news_alert(article, analysis, price_lines):
    ts        = datetime.now().strftime("%H:%M:%S")
    token_str = " ".join(f"[{t}]" for t in article["tokens"])
    print("\n" + "━" * 62)
    print(f"  {ts}  {token_str}  via {article['source']}")
    print(f"  {article['title']}")
    if price_lines:
        print(f"  {' | '.join(price_lines)}")
    print()
    for line in analysis.strip().splitlines():
        print(f"  {line}")
    print("━" * 62)


def send_telegram_news_alert(article, analysis, price_lines):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    token_str     = " ".join(f"[{t}]" for t in article["tokens"])
    price_section = ("\n" + " | ".join(price_lines)) if price_lines else ""
    text = (
        f"🚨 <b>CRYPTO ALERT</b> {token_str}\n"
        f"<b>{_html_escape(article['title'])}</b>\n"
        f"<i>via {article['source']}</i>"
        f"{price_section}\n\n"
        f"{analysis.strip()}"
    )
    _tg_send(TELEGRAM_CHAT_ID, text)


def print_whale_alert(event, analysis, price_line):
    ts   = datetime.now().strftime("%H:%M:%S")
    move = format_whale_move(event)
    print("\n" + "~" * 62)
    print(f"  {ts}  🐋 WHALE ALERT  [{event['token']}]")
    print(f"  {move}")
    if price_line:
        print(f"  {price_line}")
    print()
    for line in analysis.strip().splitlines():
        print(f"  {line}")
    print("~" * 62)


def send_telegram_whale_alert(event, analysis, price_line):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    move = _html_escape(format_whale_move(event))

    # Parse analysis lines into a dict for clean formatting
    parsed = {}
    for line in analysis.strip().splitlines():
        if ":" in line:
            k, _, v = line.partition(":")
            parsed[k.strip()] = v.strip()

    signal   = parsed.get("SIGNAL", "")
    suggests = _html_escape(parsed.get("Suggests", ""))
    impact   = _html_escape(parsed.get("1-4H IMPACT", ""))
    price_section = f"\n{price_line}" if price_line else ""

    text = (
        f"🐋 <b>WHALE ALERT</b> [{event['token']}]\n"
        f"{move}"
        f"{price_section}\n\n"
        f"Suggests: {suggests}\n"
        f"Signal: {signal}\n"
        f"1-4H IMPACT: {impact}"
    )
    _tg_send(TELEGRAM_CHAT_ID, text)


# ── telegram command listener ─────────────────────────────────

def send_status_reply(chat_id, state):
    prices = fetch_prices(WATCHLIST)
    stats  = state.get_stats()
    price_lines = [
        format_price_line(t, prices[t]) if t in prices else f"{t} — unavailable"
        for t in WATCHLIST
    ]
    last_str = stats["last_alert"].strftime("%H:%M:%S") if stats["last_alert"] else "none yet"
    text = (
        f"<b>📊 Monitor Status</b>\n\n"
        f"<b>Prices (1h change)</b>\n" + "\n".join(price_lines) +
        f"\n\n<b>Last HIGH alert:</b> {last_str}\n"
        f"<b>News filtered today:</b> {stats['filtered_today']} (MEDIUM/LOW)\n"
        f"<b>Whale alerts today:</b> {stats['whale_alerts_today']} (BTC·ETH·SOL·HYPE)\n"
        f"<i>Updated {datetime.now().strftime('%H:%M:%S')}</i>"
    )
    _tg_send(chat_id, text)


def telegram_command_listener(state):
    offset = None
    while True:
        try:
            params = {"timeout": 30, "allowed_updates": ["message"]}
            if offset is not None:
                params["offset"] = offset
            resp = requests.get(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates",
                params=params, timeout=35,
            )
            for update in resp.json().get("result", []):
                offset  = update["update_id"] + 1
                msg     = update.get("message", {})
                text    = msg.get("text", "").strip().lower()
                chat_id = str(msg.get("chat", {}).get("id", ""))
                if text == "status" and chat_id:
                    send_status_reply(chat_id, state)
        except Exception:
            time.sleep(5)


# ── main loop ─────────────────────────────────────────────────

def run_monitor():
    acquire_single_instance_lock()

    state           = State()
    telegram_active = bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)
    eth_active      = bool(ETHERSCAN_API_KEY)

    print("=" * 62)
    print("  CRYPTO NEWS + WHALE MONITOR")
    print(f"  Watching : {', '.join(WATCHLIST)}")
    print(f"  Sources  : {' · '.join(f['name'] for f in RSS_FEEDS)}")
    print(f"  Whales   : BTC (Blockchain.com) | {'ETH (Etherscan)' if eth_active else 'ETH (no key)'} | SOL (Solscan) | HYPE (Hyperliquid)")
    print(f"  Interval : every {POLL_INTERVAL // 60} min  |  macro every {MACRO_INTERVAL // 3600}h  |  brief at {BRIEF_HOUR:02d}:00")
    print(f"  Telegram : {'enabled — send \"status\" for live update' if telegram_active else 'not configured'}")
    print("=" * 62)

    if telegram_active:
        threading.Thread(target=telegram_command_listener, args=(state,), daemon=True).start()
        _tg_send(TELEGRAM_CHAT_ID,
            f"✅ <b>Crypto Monitor Online</b>\n"
            f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"Watching: {', '.join(WATCHLIST)} | {len(RSS_FEEDS)} feeds\n"
            f"Whale monitoring: BTC · {'ETH' if eth_active else 'ETH(no key)'} · SOL · HYPE\n"
            f"Morning brief at {BRIEF_HOUR:02d}:00 daily\n"
            f"Send <code>status</code> for live prices &amp; stats."
        )
        print("\n  Startup message sent to Telegram.")

    print("\nChecking macro calendar...")
    macro_events = fetch_macro_events()
    print(f"  {len(macro_events)} high-impact USD event(s) in next 24h")
    send_macro_digest(macro_events)
    state.last_macro_check = time.time()

    # Seed news seen_ids
    seen_ids = load_seen_ids()
    if not seen_ids:
        print("\nFirst run — seeding existing articles...")
        articles = fetch_all_articles()
        seen_ids = {a["id"] for a in articles}
        save_seen_ids(seen_ids)
        print(f"Seeded {len(seen_ids)} articles.")
    else:
        print(f"\nLoaded {len(seen_ids)} seen articles.")

    # Seed HYPE trade IDs (so first run doesn't replay old trades)
    seen_whales = load_seen_whales()
    if not seen_whales:
        print("Seeding Hyperliquid trade history...")
        initial_trades = fetch_hype_large_trades(set())
        seen_whales = {e["id"] for e in initial_trades}
        save_seen_whales(seen_whales)
        print(f"Seeded {len(seen_whales)} HYPE trade ID(s).")

    print("\nMonitoring...\n")

    while True:
        now_ts  = time.time()
        now_dt  = datetime.now()
        now_str = now_dt.strftime("%H:%M:%S")
        today   = now_dt.date()

        # Morning brief at BRIEF_HOUR
        if now_dt.hour == BRIEF_HOUR and state.last_brief_date != today:
            print(f"\n[{now_str}] Sending morning brief...")
            try:
                send_morning_brief(state)
            except Exception as e:
                print(f"  [error] Morning brief failed: {e}")
                state.last_brief_date = today

        # Macro refresh every 4 hours
        if now_ts - state.last_macro_check >= MACRO_INTERVAL:
            print(f"\n[{now_str}] Refreshing macro calendar...")
            macro_events = fetch_macro_events()
            print(f"  {len(macro_events)} high-impact USD event(s) in next 24h")
            send_macro_digest(macro_events)
            state.last_macro_check = now_ts

        # ── funding rate monitoring (every 15 min) ────────────

        if now_ts - state.last_funding_check >= FUNDING_INTERVAL:
            rates = fetch_funding_rates()
            state.last_funding_check = now_ts
            for token, rate_pct in rates.items():
                abs_r    = abs(rate_pct)
                prev_r   = abs(state.last_funding_rates.get(token, 0.0))
                # Tier 0 = normal, 1 = alert (≥0.05%), 2 = extreme (≥0.10%)
                tier_now  = 2 if abs_r >= FUNDING_EXTREME_PCT else (1 if abs_r >= FUNDING_ALERT_PCT else 0)
                tier_prev = 2 if prev_r >= FUNDING_EXTREME_PCT else (1 if prev_r >= FUNDING_ALERT_PCT else 0)
                if tier_now == 0 or tier_now <= tier_prev:
                    continue  # below threshold or haven't escalated
                try:
                    analysis = analyse_funding_rate(token, rate_pct)
                    print_funding_alert(token, rate_pct, analysis)
                    send_telegram_funding_alert(token, rate_pct, analysis)
                    state.set_last_alert()
                except Exception as e:
                    print(f"  [error] Funding analysis failed for {token}: {e}")
            state.last_funding_rates = rates

        # ── whale monitoring ──────────────────────────────────

        # On-chain whales: BTC (Blockchain.com), ETH (Etherscan), SOL (Solscan)
        chain_prices = fetch_prices(["BTC", "ETH", "SOL"])
        chain_events = (
            fetch_btc_whales(seen_whales, chain_prices.get("BTC", {}).get("price", 0))
            + fetch_eth_whales(seen_whales, chain_prices.get("ETH", {}).get("price", 0), state)
            + fetch_sol_whales(seen_whales, chain_prices.get("SOL", {}).get("price", 0))
        )
        for event in chain_events:
            if event["id"] in seen_whales:
                continue
            seen_whales.add(event["id"])
            try:
                analysis   = analyse_whale_event(event)
                prices     = fetch_prices([event["token"]])
                price_line = format_price_line(event["token"], prices[event["token"]]) if event["token"] in prices else ""
                print_whale_alert(event, analysis, price_line)
                send_telegram_whale_alert(event, analysis, price_line)
                state.set_last_alert()
                state.increment_whale()
            except Exception as e:
                print(f"  [error] Whale analysis failed: {e}")

        # Hyperliquid large trades (HYPE)
        hype_trades = fetch_hype_large_trades(seen_whales)
        for event in hype_trades:
            seen_whales.add(event["id"])
            try:
                analysis   = analyse_whale_event(event)
                prices     = fetch_prices(["HYPE"])
                price_line = format_price_line("HYPE", prices["HYPE"]) if "HYPE" in prices else ""
                print_whale_alert(event, analysis, price_line)
                send_telegram_whale_alert(event, analysis, price_line)
                state.set_last_alert()
                state.increment_whale()
            except Exception as e:
                print(f"  [error] HYPE trade analysis failed: {e}")

        if hype_trades or chain_events:
            save_seen_whales(seen_whales)

        # ── news monitoring ───────────────────────────────────

        try:
            articles = fetch_all_articles()
            seen_ids.update(load_seen_ids())
            new_articles = [a for a in articles if a["id"] not in seen_ids]

            if new_articles:
                print(f"\n[{now_str}] {len(new_articles)} new article(s) — screening...")
                batched = []
                for article in new_articles:
                    seen_ids.add(article["id"])
                    try:
                        # Auto-HIGH for gov wires only when iran + at least one other MACRO keyword
                        if article["source"] in AUTO_HIGH_SOURCES and "MACRO" in article["tokens"]:
                            combined_text = f"{article['title']} {article.get('summary', '')}"
                            macro_hits    = {m.lower() for m in _MACRO_RE.findall(combined_text)}
                            auto_high     = bool(macro_hits - {"iran"})
                        else:
                            auto_high = False

                        sig = "HIGH" if auto_high else quick_significance(article["title"], article["tokens"], article["source"])
                        if sig == "HIGH":
                            # Dedup — skip if same story already sent
                            if _is_duplicate_headline(article["title"], state.recent_headlines):
                                print(f"  [dedup] {article['title'][:70]}")
                                continue

                            price_tokens = article["tokens"] if article["tokens"] != ["MACRO"] else ["BTC", "ETH"]
                            prices       = fetch_prices(price_tokens)
                            price_lines  = [format_price_line(t, prices[t]) for t in price_tokens if t in prices]
                            analysis     = full_analysis(article["title"], article["tokens"], article["source"])
                            print_news_alert(article, analysis, price_lines)

                            # Rate limit — send individually if under cap, else batch
                            now_ts2 = time.time()
                            state.recent_alert_times = [
                                t for t in state.recent_alert_times
                                if now_ts2 - t < ALERT_WINDOW_SECS
                            ]
                            if len(state.recent_alert_times) < ALERT_RATE_LIMIT:
                                send_telegram_news_alert(article, analysis, price_lines)
                                state.recent_alert_times.append(now_ts2)
                            else:
                                batched.append((article, analysis, price_lines))

                            state.recent_headlines.append((now_ts2, article["title"].lower()))
                            state.set_last_alert()
                        else:
                            log_filtered(article, sig)
                            state.increment_filtered()
                    except Exception as e:
                        print(f"  [error] {e}")

                if batched:
                    print(f"  [batch] {len(batched)} article(s) rate-limited, sending as batch")
                    _tg_send_batch(batched)

                save_seen_ids(seen_ids)
            else:
                print(f"[{now_str}] No new articles. Next check in {POLL_INTERVAL // 60}m...", end="\r")

        except Exception as e:
            print(f"[{now_str}] Poll error: {e}")

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    run_monitor()
