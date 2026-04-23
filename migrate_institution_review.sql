-- ============================================================
-- Migration: rebuild institution_review with new schema
-- Run this in Supabase SQL editor.
-- ============================================================

-- Step 1: Drop and recreate the table with the new columns
drop table if exists public.institution_review cascade;

create table public.institution_review (
  review_id bigserial not null,
  cw_year integer not null,
  country text null,
  country_code text null,
  institution_name_for_match text null,
  institution_name_norm text null,
  representing_institution text null,
  participation_type text null,
  n_signups integer not null default 0,
  alias_institution_id integer null,
  status text not null default 'pending'::text,
  suggested_institution_id integer null,
  suggested_institution_name text null,
  confidence integer null,
  created_at timestamp with time zone not null default now(),
  updated_at timestamp with time zone not null default now(),
  constraint institution_review_pkey primary key (review_id),
  constraint institution_review_cw_year_country_code_institution_name_no_key unique (cw_year, country_code, institution_name_norm),
  constraint institution_review_alias_institution_id_fkey foreign key (alias_institution_id) references institutions (institution_id)
) tablespace pg_default;

-- Step 2: Recreate the triggers on the new table
create trigger trg_resolve_institution_review
before update of alias_institution_id
on public.institution_review
for each row
execute function public.fn_resolve_institution_review();

create trigger trg_resolve_institution_review_alias
after update of alias_institution_id
on public.institution_review
for each row
execute function public.fn_resolve_institution_review_alias();

create trigger trg_generate_match_suggestion
after insert on public.institution_review
for each row
execute function public.fn_trigger_match_suggestion();

-- Step 3: Repopulate from existing unmatched signups
-- Group only by the unique constraint columns (cw_year, country_code, institution_name_norm)
-- and aggregate the rest to avoid duplicate constraint violations within the same INSERT.
insert into public.institution_review (
  cw_year,
  country,
  country_code,
  institution_name_for_match,
  institution_name_norm,
  representing_institution,
  participation_type,
  n_signups
)
select
  cw_year,
  max(country)                  as country,
  country_code,
  max(institution_name_for_match) as institution_name_for_match,
  institution_name_norm,
  max(representing_institution) as representing_institution,
  max(participation_type)       as participation_type,
  count(*)::integer             as n_signups
from public.signups
where institution_id is null
  and coalesce(representing_institution, 'Yes') = 'Yes'
group by
  cw_year,
  country_code,
  institution_name_norm;
