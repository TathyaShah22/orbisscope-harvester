-- OrbisScope — waitlist email capture. Run once in the Supabase SQL editor.
create table if not exists waitlist (
  id         bigint generated always as identity primary key,
  email      text not null unique,
  created_at timestamptz default now()
);
