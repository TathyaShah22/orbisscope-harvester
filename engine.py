"""
OrbisScope Refinery Engine (Tier 1 — GitHub Actions, CPU, Groq API).

The monitor -> refine loop (Fix 4): reads UNprocessed rows from raw_news_feed
(those whose id is not yet a raw_news_id in processed_events), classifies each
with Groq, geocodes the location, and writes structured intel into
processed_events.

Fixes baked in:
  - Real dedup via raw_news_id (no more duplicate map pins on every run).
  - Execution queue + exponential backoff on 429 (Fix 6) so heavy traffic
    slows the pipeline instead of falling back to stale demo data.
  - Location normalization so country names match the Mapbox fill layer.
  - event_description is populated (the Gemini path used to leave it null,
    starving the Live News panel).

The Colab notebook mirrors this but swaps Groq for a local Qwen-2.5-7B model on
the GPU as a zero-rate-limit fallback + adds embedding clustering.
"""

import os
import json

from groq import Groq

from common import (
    get_supabase, RateLimiter, process_queue, normalize_location, Geocoder,
    fetch_all,
)

BATCH = 60          # max articles to refine per run (keeps Actions runs short)
GROQ_MODEL = "llama-3.1-8b-instant"

groq_client = Groq(api_key=os.environ.get("GROQ_API_KEY"))
# Groq free tier ~30 rpm; stay comfortably under it.
limiter = RateLimiter(max_calls=25, period=60.0)
geocoder = Geocoder()

PROMPT = """You are a geopolitical intelligence analyst. Analyze this news event.

Title: {title}
Summary: {summary}

Return ONLY a valid JSON object with EXACTLY these keys:
"category": one of "MILITARY_CONFLICT", "DIPLOMATIC_TENSION", "ECONOMIC_CRISIS", "NEUTRAL_NEWS"
"tension_score": float 0.0 (total peace) to 1.0 (extreme war/crisis)
"primary_location": the single most relevant country name, or "Global"
"event_description": one concise sentence summarizing the event
"""


def classify(title, summary):
    limiter.acquire()
    resp = groq_client.chat.completions.create(
        messages=[{"role": "user", "content": PROMPT.format(title=title, summary=summary or "")}],
        model=GROQ_MODEL,
        temperature=0.1,
        response_format={"type": "json_object"},
    )
    return json.loads(resp.choices[0].message.content)


def get_unprocessed(supabase):
    # Paginate past the 1000-row cap so dedup sees every already-processed id.
    processed = fetch_all(supabase, "processed_events", "raw_news_id")
    done_ids = {r["raw_news_id"] for r in processed if r.get("raw_news_id")}

    raw = (supabase.table("raw_news_feed")
           .select("*")
           .order("created_at", desc=True)
           .limit(1000)
           .execute())
    return [r for r in (raw.data or []) if r["id"] not in done_ids][:BATCH]


def refine_one(supabase):
    def handler(article):
        intel = classify(article["title"], article.get("raw_text", ""))
        loc = normalize_location(intel.get("primary_location", "Global"))
        lat, lng = geocoder.locate(loc)
        supabase.table("processed_events").insert({
            "raw_news_id": article["id"],
            "event_type": intel.get("category", "NEUTRAL_NEWS"),
            "sentiment_score": float(intel.get("tension_score", 0.0)),
            "location_name": loc,
            "lat": lat,
            "lng": lng,
            "event_description": intel.get("event_description", article["title"]),
        }).execute()
        print(f"  [+] {intel.get('category')} {intel.get('tension_score')} | {loc}")

    return handler


def run():
    supabase = get_supabase()
    print("🧠 Refinery engine starting...")
    todo = get_unprocessed(supabase)
    if not todo:
        print("✅ Everything refined. Standing by.")
        return

    print(f"Refining {len(todo)} new intercepts...")
    handled = process_queue(
        todo,
        refine_one(supabase),
        on_error=lambda a, e: print(f"  ❌ skipped '{a['title'][:40]}': {e}"),
    )
    print(f"✅ Refinery complete. {handled}/{len(todo)} events secured.")


if __name__ == "__main__":
    run()
