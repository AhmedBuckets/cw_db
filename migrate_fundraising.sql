-- ============================================================
-- Migration: extend fundraising for trigger-driven institution review
-- Run this after the fundraising table exists.
-- Apply order:
--   1. migrate_fundraising.sql
--   2. migrate_institution_review.sql
--   3. functions.sql
--   4. fuzzy_match.sql
-- ============================================================

alter table public.fundraising
  add column if not exists institution_name_for_match text null,
  add column if not exists institution_name_norm text null,
  add column if not exists institution_match_status text null;

create index if not exists fundraising_cw_year_name_norm_idx
  on public.fundraising using btree (cw_year, institution_name_norm) tablespace pg_default;

update public.fundraising
set institution_name_for_match = nullif(btrim(coalesce(institution_name, '')), ''),
    institution_name_norm = nullif(
      trim(
        regexp_replace(
          regexp_replace(lower(coalesce(institution_name, '')), '[^a-z0-9]+', ' ', 'g'),
          '\s+',
          ' ',
          'g'
        )
      ),
      ''
    ),
    institution_match_status = case
      when institution_id is not null then 'matched'
      else institution_match_status
    end;