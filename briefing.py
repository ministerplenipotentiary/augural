import os
import json
import requests
from datetime import datetime, date, timezone
from dotenv import load_dotenv
import anthropic

load_dotenv()

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
CRYPTOPANIC_API_KEY = os.getenv("CRYPTOPANIC_API_KEY")

WATCHLIST = ["HYPE", "BTC", "ETH", "SOL"]


def fetch_macro_events():
    """Fetch this week's high/medium impact macro events from ForexFactory (no API key required)."""
    url = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"
    try:
        resp = requests.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
        resp.raise_for_status()
        events = resp.json()
        today_str = date.today().isoformat()  # "2026-04-17"
        today_events = [
            e for e in events
            if e.get("date", "").startswith(today_str)
            and e.get("impact") in ("High", "Medium")
        ]
        return today_events
    except Exception as e:
        print(f"  [warn] Macro fetch failed: {e}")
        return []


def fetch_crypto_news():
    """Fetch latest hot crypto news for watchlist from CryptoPanic."""
    currencies = ",".join(WATCHLIST)
    url = "https://cryptopanic.com/api/v1/posts/"
    params = {
        "auth_token": CRYPTOPANIC_API_KEY,
        "currencies": currencies,
        "filter": "hot",
        "public": "true",
    }
    try:
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        posts = resp.json().get("results", [])
        # Return slim dicts to keep the prompt concise
        return [
            {
                "title": p.get("title"),
                "source": p.get("source", {}).get("title"),
                "published_at": p.get("published_at"),
                "tokens": [c["code"] for c in p.get("currencies", [])],
            }
            for p in posts[:25]
        ]
    except Exception as e:
        print(f"  [warn] News fetch failed: {e}")
        return []


def build_briefing(macro_events, crypto_news):
    """Send data to Claude and get back a formatted briefing."""
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    today_label = datetime.now().strftime("%A, %B %d %Y")

    macro_section = (
        json.dumps(macro_events, indent=2)
        if macro_events
        else "No high/medium-impact macro events scheduled today."
    )
    news_section = (
        json.dumps(crypto_news, indent=2)
        if crypto_news
        else "No news retrieved."
    )

    prompt = f"""You are a sharp crypto market analyst writing a morning briefing for an experienced altcoin trader.

Today is {today_label}.

=== MACRO CALENDAR (today's high/medium impact events) ===
{macro_section}

=== CRYPTO NEWS (latest hot posts for {", ".join(WATCHLIST)}) ===
{news_section}

Write a concise daily briefing structured exactly like this:

---
📅 DAILY CRYPTO BRIEFING — {today_label}

## 1. MACRO PULSE
Summarise today's key economic events and their likely effect on risk assets / crypto. If nothing major, say so briefly.

## 2. TOKEN WATCH
For each token (BTC, ETH, SOL, HYPE), one short paragraph: what's in the news, sentiment, anything actionable.

## 3. KEY THEMES
2–3 bullet points on dominant narratives or risks across the market today.

## 4. TRADER'S EDGE
2–3 bullet points — specific things to monitor or act on today (setups, catalysts, risks to manage).
---

Be direct, specific, and skip filler. If data is thin for a token, say so honestly."""

    message = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=1800,
        messages=[{"role": "user", "content": prompt}],
    )
    return message.content[0].text


def save_briefing(text):
    today_str = date.today().isoformat()
    filename = f"briefing_{today_str}.txt"
    header = f"CRYPTO DAILY BRIEFING — {today_str}\nGenerated {datetime.now().strftime('%H:%M:%S')}\n{'='*60}\n\n"
    with open(filename, "w", encoding="utf-8") as f:
        f.write(header + text + "\n")
    return filename


def main():
    print("=" * 60)
    print("  CRYPTO BRIEFING TOOL")
    print("=" * 60)

    print("\n[1/3] Fetching macro events...")
    macro = fetch_macro_events()
    print(f"      {len(macro)} event(s) found for today")

    print("[2/3] Fetching crypto news...")
    news = fetch_crypto_news()
    print(f"      {len(news)} news items found")

    print("[3/3] Synthesising briefing with Claude...\n")
    briefing = build_briefing(macro, news)

    print("=" * 60)
    print(briefing)
    print("=" * 60)

    filename = save_briefing(briefing)
    print(f"\nSaved → {filename}")


if __name__ == "__main__":
    main()
