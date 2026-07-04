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
--    Explicit revoke of write grants is defense-in-depth: RLS with only a
--    SELECT policy should already deny writes, but revoking the underlying
--    grant means anon/authenticated cannot write even if a future policy
--    change is made carelessly.
revoke insert, update, delete on processed_events from anon, authenticated;
alter table processed_events enable row level security;
drop policy if exists "public read" on processed_events;
create policy "public read" on processed_events for select using (true);

-- 2. market_signals — public read (AI Signals, Market Intelligence).
revoke insert, update, delete on market_signals from anon, authenticated;
alter table market_signals enable row level security;
drop policy if exists "public read" on market_signals;
create policy "public read" on market_signals for select using (true);

-- 3. risk_scores — public read (Risk Radar, country dossier, GTI trend).
revoke insert, update, delete on risk_scores from anon, authenticated;
alter table risk_scores enable row level security;
drop policy if exists "public read" on risk_scores;
create policy "public read" on risk_scores for select using (true);

-- 4. risk_movement — public read (Risk Radar, country dossier).
revoke insert, update, delete on risk_movement from anon, authenticated;
alter table risk_movement enable row level security;
drop policy if exists "public read" on risk_movement;
create policy "public read" on risk_movement for select using (true);

-- 5. raw_news_feed — backend staging table, never queried by the frontend.
--    RLS enabled with zero policies = fully denied to anon/authenticated.
revoke select, insert, update, delete on raw_news_feed from anon, authenticated;
alter table raw_news_feed enable row level security;

-- 6. countries — uploaded once for potential future use, not queried by the
--    frontend today. Fully private until something actually needs it.
revoke select, insert, update, delete on countries from anon, authenticated;
alter table countries enable row level security;

-- 7. waitlist — the fix for the exposed emails/phone numbers. Public can
--    INSERT (submit the form) but can never SELECT/UPDATE/DELETE existing
--    rows. A basic format check on insert is defense-in-depth alongside the
--    frontend's own validation.
--    NOTE: the email regex avoids \s inside a bracket expression — Postgres's
--    regex engine can treat \s literally there (matching backslash/"s") rather
--    than "whitespace", which silently rejected any email containing the
--    letter s. Using explicit character ranges instead sidesteps that.
revoke select, update, delete on waitlist from anon, authenticated;
alter table waitlist enable row level security;
drop policy if exists "public insert" on waitlist;
create policy "public insert" on waitlist for insert
  with check (
    email ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
    and phone is not null and length(trim(phone)) > 0
  );
