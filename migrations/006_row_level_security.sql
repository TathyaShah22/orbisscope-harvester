-- OrbisScope — lock down Row Level Security before public deployment.
-- Run once in the Supabase SQL editor. Idempotent (safe to re-run).
--
-- IMPORTANT — read before running:
-- The backend (GitHub Actions + Colab) MUST use the Supabase **service_role**
-- key (Project Settings -> API -> service_role), not the anon key. service_role
-- always bypasses RLS, so this migration has zero effect on the automated
-- pipeline as long as SUPABASE_KEY (in GitHub Actions secrets and Colab
-- Secrets) is set to service_role. If it is currently set to the anon key,
-- update it to service_role BEFORE running this migration, or every
-- feeder/engine/predictor/risk_index/risk_tagger write will start failing.
--
-- What this does for the public (anon) role — the one shipped in the browser
-- bundle via NEXT_PUBLIC_SUPABASE_ANON_KEY:
--   processed_events, market_signals, risk_scores, risk_movement  -> read-only
--   waitlist                                                      -> insert-only
--                                                                     (cannot
--                                                                     read/update
--                                                                     /delete
--                                                                     existing
--                                                                     signups)
--   raw_news_feed, countries                                      -> no access
--                                                                     at all
--                                                                     (backend-
--                                                                     only
--                                                                     staging /
--                                                                     unused by
--                                                                     the
--                                                                     frontend)

-- 1. processed_events — public read (globe, news feed, AI signals context).
alter table processed_events enable row level security;
drop policy if exists "public read" on processed_events;
create policy "public read" on processed_events for select using (true);

-- 2. market_signals — public read (AI Signals, Market Intelligence).
alter table market_signals enable row level security;
drop policy if exists "public read" on market_signals;
create policy "public read" on market_signals for select using (true);

-- 3. risk_scores — public read (Risk Radar, country dossier, GTI trend).
alter table risk_scores enable row level security;
drop policy if exists "public read" on risk_scores;
create policy "public read" on risk_scores for select using (true);

-- 4. risk_movement — public read (Risk Radar, country dossier).
alter table risk_movement enable row level security;
drop policy if exists "public read" on risk_movement;
create policy "public read" on risk_movement for select using (true);

-- 5. raw_news_feed — backend staging table, never queried by the frontend.
--    RLS enabled with zero policies = fully denied to anon/authenticated.
alter table raw_news_feed enable row level security;

-- 6. countries — uploaded once for potential future use, not queried by the
--    frontend today. Fully private until something actually needs it.
alter table countries enable row level security;

-- 7. waitlist — the fix for the exposed emails/phone numbers. Public can
--    INSERT (submit the form) but can never SELECT/UPDATE/DELETE existing
--    rows. A basic format check on insert is defense-in-depth alongside the
--    frontend's own validation.
alter table waitlist enable row level security;
drop policy if exists "public insert" on waitlist;
create policy "public insert" on waitlist for insert
  with check (
    email ~* '^[^\s@]+@[^\s@]+\.[^\s@]+$'
    and phone is not null and length(trim(phone)) > 0
  );
