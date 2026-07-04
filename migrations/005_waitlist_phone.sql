-- OrbisScope — add phone number to waitlist. Run once in the Supabase SQL editor.
alter table waitlist
  add column if not exists phone text;
