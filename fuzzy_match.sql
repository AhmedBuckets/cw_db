-- ============================================================
-- FUZZY MATCH SUGGESTIONS FOR INSTITUTION REVIEW
-- Requires: pg_trgm extension (enabled in Supabase by default)
-- ============================================================

-- Enable if not already
create extension if not exists pg_trgm;


-- ============================================================
-- Helper: split a normalised name into meaningful tokens
-- Strips common filler words so "university of london north"
-- and "university of london south" differ on the words that
-- actually matter.
-- ============================================================
create or replace function public.fn_tokenise_institution_name(name_norm text)
returns text[]
language sql
immutable
as $$
  select array_agg(word order by word)
  from unnest(string_to_array(name_norm, ' ')) as word
  where word not in (
    -- common filler words that don't help distinguish institutions
    'the', 'of', 'and', 'for', 'in', 'at', 'de', 'du', 'des', 'la', 'le',
    'al', 'el', 'a', 'an', 'e', 'i', 'y', 'und', 'et', 'van', 'von',
    'di', 'da', 'do', 'das', 'den', 'der', 'het'
  )
  and length(word) > 1;
$$;


-- ============================================================
-- Core function: generate fuzzy match suggestions for pending
-- institution_review rows and store them in the suggestions column.
--
-- Uses three signals:
--   1. Trigram similarity  (pg_trgm) — character-level fuzziness
--   2. Token overlap ratio          — word-level precision
--   3. Levenshtein-like penalty     — length difference awareness
--
-- Also checks existing aliases in institution_aliases, not just
-- canonical names.
--
-- Parameters:
--   p_min_score    — minimum combined score to include (default 0.3)
--   p_max_results  — max suggestions per review row (default 5)
--   p_review_id    — optional: only process this one review row
-- ============================================================
create or replace function public.fn_generate_match_suggestions(
  p_min_score    float default 0.3,
  p_review_id    bigint default null
)
returns table (
  review_id           bigint,
  suggestions_count   int
)
language plpgsql
security definer
as $$
declare
  v_review record;
  v_top_institution_id   int;
  v_top_institution_name text;
  v_top_confidence       int;
begin
  -- Loop through each pending review row (or just the one specified)
  for v_review in
    select r.review_id,
        r.review_source,
        r.match_scope_key,
           r.institution_name_norm,
           r.institution_name_for_match,
           r.country_code,
           r.cw_year
    from public.institution_review r
    where r.status = 'pending'
      and (p_review_id is null or r.review_id = p_review_id)
    order by r.n_signups desc
  loop
    -- Find the top candidate from institutions + institution_aliases
    select
      c.institution_id,
      c.name_canonical,
      round(c.combined_score::numeric * 100)::int
    into v_top_institution_id, v_top_institution_name, v_top_confidence
    from (
      -- ---- Candidates from institutions table (canonical name) ----
      select
        i.institution_id,
        i.name_canonical,
        -- Combined: weighted blend (trigram 40%, token overlap 60%)
        -- Token overlap is weighted higher to penalise "University of London North"
        -- vs "University of London South" (high trigram, low token diff)
        0.4 * similarity(v_review.institution_name_norm, i.institution_name_norm)
        + 0.6 * coalesce((
          select count(*)::float / nullif(
            (select count(distinct t) from unnest(
              fn_tokenise_institution_name(v_review.institution_name_norm) ||
              fn_tokenise_institution_name(i.institution_name_norm)
            ) as t), 0
          )
          from unnest(fn_tokenise_institution_name(v_review.institution_name_norm)) as rt
          where rt = any(fn_tokenise_institution_name(i.institution_name_norm))
        ), 0) as combined_score
      from public.institutions i
      where (
          v_review.review_source = 'fundraising'
          or i.country_code = v_review.match_scope_key
        )
        and similarity(v_review.institution_name_norm, i.institution_name_norm) > 0.15

      union all

      -- ---- Candidates from institution_aliases table ----
      select
        a.institution_id,
        (select ii.name_canonical from public.institutions ii where ii.institution_id = a.institution_id),
        0.4 * similarity(v_review.institution_name_norm, a.alias_name_norm)
        + 0.6 * coalesce((
          select count(*)::float / nullif(
            (select count(distinct t) from unnest(
              fn_tokenise_institution_name(v_review.institution_name_norm) ||
              fn_tokenise_institution_name(a.alias_name_norm)
            ) as t), 0
          )
          from unnest(fn_tokenise_institution_name(v_review.institution_name_norm)) as rt
          where rt = any(fn_tokenise_institution_name(a.alias_name_norm))
        ), 0) as combined_score
      from public.institution_aliases a
      where (
          v_review.review_source = 'fundraising'
          or a.country_code = v_review.match_scope_key
        )
        and similarity(v_review.institution_name_norm, a.alias_name_norm) > 0.15
    ) c
    where c.combined_score >= p_min_score
    order by c.combined_score desc
    limit 1;

    -- Update the review row with the top suggestion
    update public.institution_review r
    set suggested_institution_id   = v_top_institution_id,
        suggested_institution_name = v_top_institution_name,
        confidence                 = v_top_confidence,
        updated_at                 = now()
    where r.review_id = v_review.review_id;

    -- Return progress (1 if a suggestion was found, 0 otherwise)
    review_id         := v_review.review_id;
    suggestions_count := case when v_top_institution_id is not null then 1 else 0 end;
    return next;
  end loop;
end;
$$;


-- ============================================================
-- Trigger: auto-run fuzzy matching on new institution_review rows
-- ============================================================
create or replace function public.fn_trigger_match_suggestion()
returns trigger
language plpgsql
security definer
as $$
begin
  perform public.fn_generate_match_suggestions(p_review_id := new.review_id);
  return new;
end;
$$;

drop trigger if exists trg_generate_match_suggestion on public.institution_review;

create trigger trg_generate_match_suggestion
after insert on public.institution_review
for each row
execute function public.fn_trigger_match_suggestion();


-- ============================================================
-- Auto-accept suggestions at or above a confidence threshold.
-- Sets alias_institution_id, which fires trg_resolve_institution_review
-- to backfill signups, mark the row resolved, and record the alias.
--
-- Parameters:
--   p_threshold  — minimum confidence (0–100) to auto-accept (default 65)
--   p_cw_year    — optional: restrict to a specific year
--
-- Returns one row per accepted review.
-- ============================================================
create or replace function public.fn_auto_accept_suggestions(
  p_threshold  int  default 65,
  p_cw_year    int  default null
)
returns table (
  review_id                bigint,
  institution_name_norm    text,
  accepted_institution_id  int,
  accepted_institution_name text,
  confidence               int
)
language plpgsql
security definer
as $$
begin
  return query
  update public.institution_review r
  set alias_institution_id = r.suggested_institution_id
  where r.status                  = 'pending'
    and r.review_source           = 'signup'
    and r.confidence              >= p_threshold
    and r.suggested_institution_id is not null
    and (p_cw_year is null or r.cw_year = p_cw_year)
  returning
    r.review_id,
    r.institution_name_norm,
    r.suggested_institution_id,
    r.suggested_institution_name,
    r.confidence;
end;
$$;


-- ============================================================
-- Convenience view: pending reviews with their top suggestion
-- ============================================================
create or replace view public.v_institution_review_suggestions as
select
  r.review_id,
  r.cw_year,
  r.country_code,
  r.institution_name_for_match   as unmatched_name,
  r.institution_name_norm,
  r.n_signups,
  r.status,
  r.suggested_institution_id,
  r.suggested_institution_name,
  r.confidence,
  r.review_source,
  r.match_scope_key
from public.institution_review r
where r.status = 'pending'
order by
  r.confidence desc nulls last,
  r.n_signups desc;
