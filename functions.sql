create or replace function public.fn_resolve_institution_review()
returns trigger
language plpgsql
security definer
as $$
begin
  -- Only act when alias_institution_id changes from NULL -> some value
  if new.alias_institution_id is not null
     and (old.alias_institution_id is null
          or old.alias_institution_id <> new.alias_institution_id) then

    -- 1) Update all matching signups that are currently unmatched
    update public.signups s
    set institution_id           = new.alias_institution_id,
        institution_match_status = 'matched'
    where s.institution_id is null
      and s.cw_year      = new.cw_year
      and s.country_code = new.country_code
      and s.institution_name_norm = new.institution_name_norm;

    -- 2) Mark this review row as resolved (works because this is a BEFORE trigger)
    new.status     := 'resolved';
    new.updated_at := now();
  end if;

  return new;
end;
$$;

-- Records the alias AFTER the review row is committed as 'resolved',
-- so the cascading alias trigger won't try to re-update this row.
create or replace function public.fn_resolve_institution_review_alias()
returns trigger
language plpgsql
security definer
as $$
begin
  if new.alias_institution_id is not null
     and (old.alias_institution_id is null
          or old.alias_institution_id <> new.alias_institution_id) then

    insert into public.institution_aliases (
      institution_id,
      country_code,
      alias_name,
      alias_name_norm
    ) values (
      new.alias_institution_id,
      new.country_code,
      new.institution_name_for_match,
      new.institution_name_norm
    )
    on conflict (institution_id, alias_name_norm) do nothing;
  end if;

  return null; -- AFTER trigger, return value is ignored
end;
$$;

drop trigger if exists trg_resolve_institution_review on public.institution_review;
drop trigger if exists trg_resolve_institution_review_alias on public.institution_review;

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





create or replace function public.fn_capture_unmatched_institution()
returns trigger
language plpgsql
security definer
as $$
begin
  -- condition: no institution_id AND they’re representing an institution
  if new.institution_id is null
     and coalesce(new.representing_institution, 'Yes') = 'Yes'
  then
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
    values (
      new.cw_year,
      new.country,
      new.country_code,
      new.institution_name_for_match,
      new.institution_name_norm,
      new.representing_institution,
      new.participation_type,
      1
    )
    on conflict (cw_year, country_code, institution_name_norm)
    do update
      set n_signups = institution_review.n_signups + 1,
          updated_at = now();
  end if;

  return new;
end;
$$;

drop trigger if exists trg_capture_unmatched_institution on public.signups;
create trigger trg_capture_unmatched_institution
after insert on public.signups
for each row
execute function public.fn_capture_unmatched_institution();



create or replace function public.fn_auto_match_on_new_institution()
returns trigger
language plpgsql
security definer
as $$
begin
  -- 1) Match any unmatched signups whose normalised name + country match
  update public.signups s
  set institution_id           = new.institution_id,
      institution_match_status = 'matched'
  where s.institution_id is null
    and s.country_code         = new.country_code
    and s.institution_name_norm = new.institution_name_norm;

  -- 2) Resolve any pending institution_review rows for the same name + country
  update public.institution_review r
  set alias_institution_id = new.institution_id,
      status               = 'resolved',
      updated_at           = now()
  where r.status                = 'pending'
    and r.country_code          = new.country_code
    and r.institution_name_norm = new.institution_name_norm
    and r.alias_institution_id  is null;

  return new;
end;
$$;

drop trigger if exists trg_auto_match_on_new_institution on public.institutions;

create trigger trg_auto_match_on_new_institution
after insert on public.institutions
for each row
execute function public.fn_auto_match_on_new_institution();



create or replace function public.fn_auto_match_on_alias_insert()
returns trigger
language plpgsql
security definer
as $$
begin
  -- Skip if the alias has no usable norm name
  if new.alias_name_norm is null then
    return new;
  end if;

  -- 1) Match any unmatched signups whose norm name + country match this alias
  update public.signups s
  set institution_id           = new.institution_id,
      institution_match_status = 'matched'
  where s.institution_id is null
    and s.institution_name_norm = new.alias_name_norm
    and s.country_code          = new.country_code;

  -- 2) Resolve any pending institution_review rows for this alias
  update public.institution_review r
  set alias_institution_id = new.institution_id,
      status               = 'resolved',
      updated_at           = now()
  where r.status                = 'pending'
    and r.alias_institution_id  is null
    and r.institution_name_norm = new.alias_name_norm
    and r.country_code          = new.country_code;

  return new;
end;
$$;

drop trigger if exists trg_auto_match_on_alias_insert on public.institution_aliases;

create trigger trg_auto_match_on_alias_insert
after insert on public.institution_aliases
for each row
execute function public.fn_auto_match_on_alias_insert();
