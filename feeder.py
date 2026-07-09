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
    "UN News": "https://news.un.org/feed/subscribe/en/news/all/rss.xml",
    # Regional
    "Moscow Times": "https://www.themoscowtimes.com/rss/news",
    "Middle East Eye": "https://www.middleeasteye.net/rss",
    "SCMP": "https://www.scmp.com/rss/91/feed",
    "The Diplomat": "https://thediplomat.com/feed/",
    "AllAfrica": "https://allafrica.com/tools/headlines/rdf/latest/headlines.rdf",
    # Conflict & security analysis
    "War on the Rocks": "https://warontherocks.com/feed/",
    "Bellingcat": "https://www.bellingcat.com/feed/",
    "Long War Journal": "https://www.longwarjournal.org/feed",
    # Policy & think tank
    "Foreign Policy": "https://foreignpolicy.com/feed/",
    "Foreign Affairs": "https://www.foreignaffairs.com/rss.xml",
    # Defense & Cyber
    "Defense News": "https://www.defensenews.com/arc/outboundfeeds/rss/?outputType=xml",
    "The Hacker News": "https://feeds.feedburner.com/TheHackersNews",
    # Markets & energy
    "Yahoo Finance": "https://finance.yahoo.com/news/rssindex",
    "MarketWatch": "http://feeds.marketwatch.com/marketwatch/topstories/",
    "OilPrice": "https://oilprice.com/rss/main",
    "EIA": "https://www.eia.gov/rss/todayinenergy.xml",
}

PER_FEED = 15  # newest N per feed per run


def harvest():
    supabase = get_supabase()
    print(f"[{now_iso()}] 📡 Feeder starting...")

    # Collect this sweep's articles, deduped within the batch by url. The DB's
    # unique constraint on url handles cross-run dedup (via upsert below), so we
    # don't need to prefetch every existing url (which the 1000-row cap made
    # unreliable anyway).
    batch = {}
    for source, url in RSS_FEEDS.items():
        try:
            feed = feedparser.parse(url)
        except Exception as e:  # noqa: BLE001
            print(f"  ⚠️ failed to parse {source}: {e}")
            continue

        for entry in feed.entries[:PER_FEED]:
            link = entry.get("link")
            if not link or link in batch:
                continue
            batch[link] = {
                "source": source,
                "title": entry.get("title", ""),
                "url": link,
                "source_url": url,
                "published_at": now_iso(),
                "raw_text": entry.get("summary", entry.get("description", "")),
            }

    new_rows = list(batch.values())
    if not new_rows:
        print("  ⏸ No articles fetched this sweep.")
        return

    print(f"  📥 Upserting {len(new_rows)} articles (existing urls ignored)...")
    for i in range(0, len(new_rows), 200):
        # ignore_duplicates -> ON CONFLICT (url) DO NOTHING, so already-seen
        # articles are skipped instead of raising a 23505 unique violation.
        (supabase.table("raw_news_feed")
         .upsert(new_rows[i:i + 200], on_conflict="url", ignore_duplicates=True)
         .execute())
    print("  ✅ Feeder complete.")


if __name__ == "__main__":
    harvest()
