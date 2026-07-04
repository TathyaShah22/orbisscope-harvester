-- OrbisScope — waitlist table + phone number. Run once in the Supabase SQL
-- editor. Self-contained: creates the table if migration 003 was never run,
-- and adds the phone column either way. Safe to re-run.
create table if not exists waitlist (
  id         bigint generated always as identity primary key,
  email      text not null unique,
  created_at timestamptz default now()
);

alter table waitlist
  add column if not exists phone text;
