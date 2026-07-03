# orbisscope-harvester

Backend data pipeline for **OrbisScope**. Two-tier design:

## Tier 1 — 24/7, CPU (this repo, GitHub Actions)

| Script | Role | Writes to |
|---|---|---|
| `feeder.py` | Pull global RSS feeds, dedup on URL | `raw_news_feed` |
| `engine.py` | AI refinery: classify unprocessed articles with Groq, geocode, score tension | `processed_events` |
| `predictor.py` | GTI aggregation + LightGBM → actionable market signals | `market_signals` |
| `common.py` | Shared clients, rate limiter, execution queue, geocoding, location normalization |

**Schedules:** `orbis-engine.yml` runs feeder+engine every 15 min; `harvest.yml` runs the predictor hourly.

Key properties:
- **Real dedup** via `raw_news_id` (engine) and `url` (feeder) — no duplicate events.
- **Execution queue + exponential backoff** on 429s, so heavy traffic *slows* the pipeline instead of falling back to stale demo data.
- **Location normalization** so country names match the Mapbox fill layer.

## Tier 2 — GPU (Colab Pro, `../OrbisScope.ipynb`)

Higher-quality refinement + clustering when a GPU box is up:
- Groq API primary, **local Qwen-2.5-7B on the GPU as the 429 fallback**.
- GPU sentence-embeddings + KMeans → `cluster_id` (emerging macro narratives).
- Same LightGBM predictor.

## Setup

```bash
pip install -r requirements.txt
```

Env / GitHub secrets: `SUPABASE_URL`, `SUPABASE_KEY` (service role), `GROQ_API_KEY`.

## Tables

`raw_news_feed` → `processed_events` (map events + GTI) → `market_signals` (AI Signals panel). `countries` holds geometry for the frontend drill-down.
