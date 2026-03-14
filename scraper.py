import feedparser
import os
from supabase import create_client, Client
from datetime import datetime, timezone

# Load Secrets from GitHub Environment
url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(url, key)

RSS_FEEDS = {
    "BBC World": "http://feeds.bbci.co.uk/news/world/rss.xml",
    "Reuters": "https://www.reutersagency.com/feed/",
    "Al Jazeera": "https://www.aljazeera.com/xml/rss/all.xml",
    "Yahoo Finance": "https://finance.yahoo.com/news/rssindex"
}

def harvest():
    print(f"[{datetime.now()}] Starting Harvester...")
    for source, feed_url in RSS_FEEDS.items():
        feed = feedparser.parse(feed_url)
        for entry in feed.entries[:10]:
            data = {
                "source": source,
                "title": entry.get("title", ""),
                "url": entry.get("link", ""),
                "published_at": datetime.now(timezone.utc).isoformat(),
                "raw_text": entry.get("summary", "")
            }
            try:
                # UNIQUE constraint on 'url' handles duplicates automatically
                supabase.table("raw_news_feed").insert(data).execute()
            except:
                continue # Skip duplicates silently
    print("Harvesting complete.")

if __name__ == "__main__":
    harvest()
