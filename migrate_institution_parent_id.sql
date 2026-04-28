-- ============================================================
-- Migration: add institutions.parent_id and backfill it from
-- institution_id for all existing rows.
-- Reapply functions.sql after this migration so the insert-time
-- initializer trigger is present on live databases.
-- ============================================================

alter table public.institutions
  add column if not exists parent_id integer;

update public.institutions
set parent_id = institution_id
where parent_id is null;