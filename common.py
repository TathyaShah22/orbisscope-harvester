"""
OrbisScope shared backend utilities.

Used by feeder.py (RSS ingest), engine.py (AI refinery) and predictor.py
(LightGBM market signals). Keeps clients, rate limiting, the execution queue,
location normalization and geocoding in one place so the deployed (GitHub
Actions) tier and the Colab GPU notebook stay consistent.

Required env vars:
    SUPABASE_URL, SUPABASE_KEY   (service-role key for inserts)
    GROQ_API_KEY                 (only needed by engine.py)
"""

import os
import time
import collections
from datetime import datetime, timezone

from supabase import create_client, Client

# --------------------------------------------------------------------------
# Clients
# --------------------------------------------------------------------------

def get_supabase() -> Client:
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")
    if not url or not key:
        raise RuntimeError("SUPABASE_URL and SUPABASE_KEY must be set.")
    return create_client(url, key)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# --------------------------------------------------------------------------
# Source weighting — approximate BGRI's "brokerage reports > public news" by
# weighting market-facing feeds above general world news.
# --------------------------------------------------------------------------

SOURCE_WEIGHTS = {
    "MarketWatch": 1.5,
    "Yahoo Finance": 1.5,
    "Defense News": 1.3,
    "The Hacker News": 1.2,
    "BBC World": 1.0,
    "New York Times": 1.0,
    "Al Jazeera": 1.0,
    "The Guardian": 1.0,
}


def source_weight(name: str) -> float:
    return SOURCE_WEIGHTS.get(name, 1.0)


def fetch_all(supabase, table, columns="*", order_col=None, desc=True, page_size=1000):
    """
    Fetch ALL rows from a table, paginating around PostgREST's default 1000-row
    response cap. Without this, .limit(50000) silently returns only 1000 rows —
    which breaks dedup and starves aggregations.
    """
    rows, start = [], 0
    while True:
        q = supabase.table(table).select(columns)
        if order_col:
            q = q.order(order_col, desc=desc)
        batch = (q.range(start, start + page_size - 1).execute().data) or []
        rows.extend(batch)
        if len(batch) < page_size:
            return rows
        start += page_size


# --------------------------------------------------------------------------
# Rate limiter (token bucket) — paces API calls so we never trip 429s.
# --------------------------------------------------------------------------

class RateLimiter:
    """Simple sliding-window limiter: at most `max_calls` per `period` seconds."""

    def __init__(self, max_calls: int, period: float = 60.0):
        self.max_calls = max_calls
        self.period = period
        self._calls = collections.deque()

    def acquire(self):
        while True:
            now = time.monotonic()
            # drop timestamps outside the window
            while self._calls and now - self._calls[0] >= self.period:
                self._calls.popleft()
            if len(self._calls) < self.max_calls:
                self._calls.append(now)
                return
            sleep_for = self.period - (now - self._calls[0]) + 0.05
            time.sleep(max(sleep_for, 0.1))


# --------------------------------------------------------------------------
# Execution queue — bounded, memory-safe work processing (Fix 6).
# Processes items one at a time with a retry/backoff policy so a burst of
# 17k raw articles never loads everything into memory or hammers an API.
# --------------------------------------------------------------------------

def process_queue(items, handler, on_error=None, max_retries=3, base_backoff=5.0):
    """
    Feed `items` through `handler(item)` one at a time.

    - handler may raise. On a rate-limit style error (429 / RESOURCE_EXHAUSTED)
      we back off exponentially and retry the SAME item (no data lost, no
      fallback to stale demo data).
    - Other errors are retried up to `max_retries`, then skipped via on_error.
    Returns the number of successfully handled items.
    """
    queue = collections.deque(items)
    done = 0
    while queue:
        item = queue.popleft()
        attempt = 0
        while True:
            try:
                handler(item)
                done += 1
                break
            except Exception as e:  # noqa: BLE001 - we classify below
                msg = str(e)
                is_rate = "429" in msg or "RESOURCE_EXHAUSTED" in msg or "rate" in msg.lower()
                attempt += 1
                if is_rate:
                    wait = base_backoff * (2 ** min(attempt, 5))
                    print(f"  ⏳ rate-limited, cooling down {wait:.0f}s (attempt {attempt})")
                    time.sleep(wait)
                    # rate-limit retries don't count against max_retries
                    attempt -= 1
                    continue
                if attempt >= max_retries:
                    if on_error:
                        on_error(item, e)
                    break
                time.sleep(base_backoff)
    return done


# --------------------------------------------------------------------------
# Location normalization — align LLM output with Mapbox `name_en` values so
# the country-fill layer and country drill-down actually match.
# --------------------------------------------------------------------------

# Common LLM variants -> canonical Mapbox English country name.
LOCATION_ALIASES = {
    "usa": "United States",
    "u.s.": "United States",
    "u.s.a.": "United States",
    "us": "United States",
    "united states of america": "United States",
    "america": "United States",
    "uk": "United Kingdom",
    "u.k.": "United Kingdom",
    "britain": "United Kingdom",
    "great britain": "United Kingdom",
    "england": "United Kingdom",
    "south korea": "South Korea",
    "north korea": "North Korea",
    "russia": "Russia",
    "russian federation": "Russia",
    "uae": "United Arab Emirates",
    "drc": "Democratic Republic of the Congo",
    "dr congo": "Democratic Republic of the Congo",
    "czech republic": "Czechia",
    "burma": "Myanmar",
    "palestine": "Palestine",
    "palestinian territories": "Palestine",
    "gaza": "Palestine",
    "west bank": "Palestine",
}

# Region / non-country tokens that should NOT be geocoded to a single point.
REGION_TOKENS = {
    "global", "world", "middle east", "europe", "asia", "africa",
    "north america", "south america", "latin america", "eu",
    "european union", "balkans", "scandinavia", "central asia",
}


def normalize_location(name: str) -> str:
    if not name:
        return "Global"
    key = name.strip().lower()
    if key in REGION_TOKENS:
        return name.strip().title() if key != "global" else "Global"
    return LOCATION_ALIASES.get(key, name.strip())


def is_region(name: str) -> bool:
    return (name or "").strip().lower() in REGION_TOKENS


# --------------------------------------------------------------------------
# Geocoding — cached, with static centroids for the hottest countries so we
# rarely hit Nominatim (which is limited to ~1 req/sec).
# --------------------------------------------------------------------------

STATIC_CENTROIDS = {
    "United States": (39.8283, -98.5795),
    "United Kingdom": (55.3781, -3.4360),
    "Iran": (32.4279, 53.6880),
    "Israel": (31.0461, 34.8516),
    "Lebanon": (33.8547, 35.8623),
    "Palestine": (31.9522, 35.2332),
    "Ukraine": (48.3794, 31.1656),
    "Russia": (61.5240, 105.3188),
    "China": (35.8617, 104.1954),
    "India": (20.5937, 78.9629),
    "Afghanistan": (33.9391, 67.7100),
    "Iraq": (33.2232, 43.6793),
    "Qatar": (25.3548, 51.1839),
    "France": (46.2276, 2.2137),
    "Cuba": (21.5218, -77.7812),
    "Kenya": (0.0236, 37.9062),
    "Mexico": (23.6345, -102.5528),
    "Canada": (56.1304, -106.3468),
    "Germany": (51.1657, 10.4515),
    "Japan": (36.2048, 138.2529),
    "Taiwan": (23.6978, 120.9605),
    "North Korea": (40.3399, 127.5101),
    "South Korea": (35.9078, 127.7669),
    "Syria": (34.8021, 38.9968),
    "Yemen": (15.5527, 48.5164),
    "Pakistan": (30.3753, 69.3451),
}


class Geocoder:
    """Lazy Nominatim wrapper with in-memory cache and static overrides."""

    def __init__(self):
        self._cache = dict(STATIC_CENTROIDS)
        self._geolocator = None

    def _ensure(self):
        if self._geolocator is None:
            from geopy.geocoders import Nominatim
            self._geolocator = Nominatim(user_agent="orbisscope_terminal")

    def locate(self, name: str):
        """Return (lat, lng). (0.0, 0.0) for regions / failures."""
        name = normalize_location(name)
        if is_region(name):
            return 0.0, 0.0
        if name in self._cache:
            return self._cache[name]
        try:
            self._ensure()
            time.sleep(1.1)  # respect Nominatim's 1 req/sec policy
            g = self._geolocator.geocode(name)
            coords = (g.latitude, g.longitude) if g else (0.0, 0.0)
        except Exception as e:  # noqa: BLE001
            print(f"  ⚠️ geocode failed for {name}: {e}")
            coords = (0.0, 0.0)
        self._cache[name] = coords
        return coords
