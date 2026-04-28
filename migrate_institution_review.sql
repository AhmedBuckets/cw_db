-- ============================================================
-- Migration: evolve institution_review in place for shared signup
-- and fundraising review rows.
-- Run this after migrate_fundraising.sql and before reapplying
-- functions.sql / fuzzy_match.sql.
-- Apply order:
--   1. migrate_fundraising.sql
--   2. migrate_institution_review.sql
--   3. functions.sql
--   4. fuzzy_match.sql
-- ============================================================

-- Step 1: add the new shared-queue columns without dropping data.
alter table public.institution_review
  add column if not exists review_source text,
  add column if not exists match_scope_key text,
  add column if not exists resolve_via text null;

alter table public.institution_review
  alter column review_source set default 'signup'::text,
  alter column match_scope_key set default ''::text;

update public.institution_review
set review_source = coalesce(review_source, 'signup');

update public.institution_review
set match_scope_key = case
  when review_source = 'signup' then coalesce(country_code, '')
  else coalesce(match_scope_key, '')
end
where match_scope_key is null
   or (review_source = 'signup' and match_scope_key <> coalesce(country_code, ''));

alter table public.institution_review
  alter column review_source set not null,
  alter column match_scope_key set not null;

-- Step 2: collapse any duplicates that become visible once NULL country_code
-- values are normalized into the shared match_scope_key.
with duplicate_groups as (
  select
    review_source,
    cw_year,
    match_scope_key,
    institution_name_norm,
    min(review_id) as keep_review_id,
    sum(n_signups)::integer as merged_n_signups,
    max(country) as merged_country,
    max(country_code) as merged_country_code,
    max(institution_name_for_match) as merged_name_for_match,
    max(representing_institution) as merged_representing_institution,
    max(participation_type) as merged_participation_type,
    max(alias_institution_id) as merged_alias_institution_id,
    case when bool_or(status = 'resolved') then 'resolved' else 'pending' end as merged_status,
    max(suggested_institution_id) as merged_suggested_institution_id,
    max(suggested_institution_name) as merged_suggested_institution_name,
    max(confidence) as merged_confidence,
    min(created_at) as merged_created_at,
    max(updated_at) as merged_updated_at
  from public.institution_review
  group by review_source, cw_year, match_scope_key, institution_name_norm
  having count(*) > 1
),
updated_groups as (
  update public.institution_review review_row
  set country                    = duplicate_groups.merged_country,
      country_code               = duplicate_groups.merged_country_code,
      institution_name_for_match = duplicate_groups.merged_name_for_match,
      representing_institution   = duplicate_groups.merged_representing_institution,
      participation_type         = duplicate_groups.merged_participation_type,
      n_signups                  = duplicate_groups.merged_n_signups,
      alias_institution_id       = duplicate_groups.merged_alias_institution_id,
      status                     = duplicate_groups.merged_status,
      suggested_institution_id   = duplicate_groups.merged_suggested_institution_id,
      suggested_institution_name = duplicate_groups.merged_suggested_institution_name,
      confidence                 = duplicate_groups.merged_confidence,
      created_at                 = duplicate_groups.merged_created_at,
      updated_at                 = duplicate_groups.merged_updated_at
  from duplicate_groups
  where review_row.review_id = duplicate_groups.keep_review_id
  returning duplicate_groups.review_source,
            duplicate_groups.cw_year,
            duplicate_groups.match_scope_key,
            duplicate_groups.institution_name_norm,
            duplicate_groups.keep_review_id
)
delete from public.institution_review review_row
using updated_groups
where review_row.review_source = updated_groups.review_source
  and review_row.cw_year = updated_groups.cw_year
  and review_row.match_scope_key = updated_groups.match_scope_key
  and review_row.institution_name_norm = updated_groups.institution_name_norm
  and review_row.review_id <> updated_groups.keep_review_id;

-- Step 3: replace the old signup-only uniqueness rule with the shared rule.
alter table public.institution_review
  drop constraint if exists institution_review_cw_year_country_code_institution_name_no_key;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'institution_review_source_cw_year_scope_name_key'
      and conrelid = 'public.institution_review'::regclass
  ) then
    alter table public.institution_review
      add constraint institution_review_source_cw_year_scope_name_key
      unique (review_source, cw_year, match_scope_key, institution_name_norm);
  end if;
end;
$$;

-- Step 4: backfill any missing signup review rows into the shared queue.
insert into public.institution_review (
  review_source,
  cw_year,
  match_scope_key,
  country,
  country_code,
  institution_name_for_match,
  institution_name_norm,
  representing_institution,
  participation_type,
  n_signups
)
select
  'signup' as review_source,
  s.cw_year,
  coalesce(s.country_code, '') as match_scope_key,
  max(s.country) as country,
  s.country_code,
  max(s.institution_name_for_match) as institution_name_for_match,
  s.institution_name_norm,
  max(s.representing_institution) as representing_institution,
  max(s.participation_type) as participation_type,
  count(*)::integer as n_signups
from public.signups s
where s.institution_id is null
  and s.institution_name_norm is not null
  and coalesce(s.representing_institution, 'Yes') = 'Yes'
group by
  s.cw_year,
  coalesce(s.country_code, ''),
  s.country_code,
  s.institution_name_norm
on conflict (review_source, cw_year, match_scope_key, institution_name_norm)
do update
  set n_signups = greatest(institution_review.n_signups, excluded.n_signups),
      updated_at = now();

-- Step 5: backfill fundraising review rows from existing unmatched data.
insert into public.institution_review (
  review_source,
  cw_year,
  match_scope_key,
  institution_name_for_match,
  institution_name_norm,
  n_signups
)
select
  'fundraising' as review_source,
  f.cw_year,
  '' as match_scope_key,
  max(f.institution_name_for_match) as institution_name_for_match,
  f.institution_name_norm,
  count(*)::integer as n_signups
from public.fundraising f
where f.institution_id is null
  and f.institution_name_norm is not null
group by
  f.cw_year,
  f.institution_name_norm
on conflict (review_source, cw_year, match_scope_key, institution_name_norm)
do update
  set n_signups = greatest(institution_review.n_signups, excluded.n_signups),
      updated_at = now();

-- Step 6: recreate the review triggers against the evolved table.
drop trigger if exists trg_resolve_institution_review on public.institution_review;
create trigger trg_resolve_institution_review
before update of alias_institution_id
on public.institution_review
for each row
execute function public.fn_resolve_institution_review();

drop trigger if exists trg_resolve_institution_review_alias on public.institution_review;
create trigger trg_resolve_institution_review_alias
after update of alias_institution_id
on public.institution_review
for each row
execute function public.fn_resolve_institution_review_alias();

drop trigger if exists trg_generate_match_suggestion on public.institution_review;
create trigger trg_generate_match_suggestion
after insert on public.institution_review
for each row
execute function public.fn_trigger_match_suggestion();
