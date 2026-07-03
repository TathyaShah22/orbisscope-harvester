-- OrbisScope — BGRI-style risk system (Tier A + C)
-- Run this once in the Supabase SQL editor. Safe / idempotent.
-- Columns are nullable so the existing pipeline + old rows keep working; the
-- new signals are populated going forward and the aggregator falls back to
-- sentiment_score for historical rows.

-- 1. Per-event signals: split attention (relevance) from direction (sentiment).
alter table processed_events
  add column if not exists relevance real,           -- 0..1  how central the event is to a market risk
  add column if not exists sentiment_signed real,    -- -1 (escalation/negative) .. +1 (de-escalation/positive)
  add column if not exists source_weight real default 1.0;  -- market-facing sources weighted higher

-- 2. Daily attention time series, z-scored vs an EWMA history.
--    scope = 'GLOBAL' or a country name. attention_z is the headline "GTI".
create table if not exists risk_scores (
  id           bigint generated always as identity primary key,
  scope        text        not null,
  day          date        not null,
  risk_level   real,        -- raw attention * negativity magnitude
  net_sentiment real,       -- -1..1 weighted average
  attention_z  real,        -- standardized vs EWMA history (0 = normal, +1 = 1 sigma high)
  event_count  int,
  updated_at   timestamptz default now(),
  unique (scope, day)
);
create index if not exists risk_scores_scope_day_idx on risk_scores (scope, day desc);

-- 3. "Priced-in" market-movement snapshot per scope
--    (event_type e.g. MILITARY_CONFLICT, a country name, or 'GLOBAL').
create table if not exists risk_movement (
  id            bigint generated always as identity primary key,
  scope         text        not null unique,
  similarity    real,       -- -1..1  cosine(actual 1m returns, MDS shock vector)
  magnitude     real,       -- 0..1   normalized size of the move
  movement_index real,      -- -1..1  combined: +1 priced-in, 0 no resemblance, -1 betting against
  updated_at    timestamptz default now()
);
