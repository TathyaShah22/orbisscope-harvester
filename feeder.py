"""
OrbisScope Feeder (Tier 1 — runs 24/7 on GitHub Actions, no GPU).

Pulls global RSS feeds and drops raw articles into `raw_news_feed`, skipping
anything we already have (dedup on url). This is intentionally dumb + cheap so
the archive keeps filling even when the Colab GPU box is offline. The AI
refinery (engine.py) consumes from here.
"""

import feedparser

from common import get_supabase, now_iso

RSS_FEEDS = {
    # Geopolitics
    "BBC World": "http://feeds.bbci.co.uk/news/world/rss.xml",
    "Al Jazeera": "https://www.aljazeera.com/xml/rss/all.xml",
    "New York Times": "https://rss.nytimes.com/services/xml/rss/nyt/World.xml",
    "The Guardian": "https://www.theguardian.com/world/rss",
    # Defense & Cyber
    "Defense News": "https://www.defensenews.com/arc/outboundfeeds/rss/?outputType=xml",
    "The Hacker News": "https://feeds.feedburner.com/TheHackersNews",
    # Markets
    "Yahoo Finance": "https://finance.yahoo.com/news/rssindex",
    "MarketWatch": "http://feeds.marketwatch.com/marketwatch/topstories/",
}

PER_FEED = 15  # newest N per feed per run


def harvest():
    supabase = get_supabase()
    print(f"[{now_iso()}] 📡 Feeder starting...")

    # Existing URLs (dedup). raw_news_feed has ~17k rows; pull just the url column.
    existing = supabase.table("raw_news_feed").select("url").limit(50000).execute()
    seen = {row["url"] for row in (existing.data or []) if row.get("url")}
    print(f"  {len(seen)} known URLs loaded for dedup.")

    new_rows = []
    for source, url in RSS_FEEDS.items():
        try:
            feed = feedparser.parse(url)
        except Exception as e:  # noqa: BLE001
            print(f"  ⚠️ failed to parse {source}: {e}")
            continue

        for entry in feed.entries[:PER_FEED]:
            link = entry.get("link")
            if not link or link in seen:
                continue
            seen.add(link)
            new_rows.append({
                "source": source,
                "title": entry.get("title", ""),
                "url": link,
                "source_url": url,
                "published_at": now_iso(),
                "raw_text": entry.get("summary", entry.get("description", "")),
            })

    if not new_rows:
        print("  ⏸ No new articles this sweep.")
        return

    print(f"  📥 Inserting {len(new_rows)} new articles...")
    # Chunk inserts to keep payloads small.
    for i in range(0, len(new_rows), 200):
        supabase.table("raw_news_feed").insert(new_rows[i:i + 200]).execute()
    print("  ✅ Feeder complete.")


if __name__ == "__main__":
    harvest()
