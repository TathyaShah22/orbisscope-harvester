-- OrbisScope — Tier B: named-risk tagging.
-- Run once in the Supabase SQL editor. Safe / idempotent.
-- Each event gets tagged with the geopolitical risk it most resembles
-- (embedding cosine similarity), enabling per-risk attention z-scores.

alter table processed_events
  add column if not exists risk_id text,          -- slug e.g. 'US_CHINA', 'MIDDLE_EAST', or 'OTHER'
  add column if not exists risk_relevance real;   -- 0..1 cosine similarity to that risk's prototype

create index if not exists processed_events_risk_id_idx on processed_events (risk_id);
