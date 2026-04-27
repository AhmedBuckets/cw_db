create or replace function public.fn_normalize_institution_name(input_name text)
returns text
language sql
immutable
as $$
  select nullif(
    trim(
      regexp_replace(
        regexp_replace(lower(coalesce(input_name, '')), '[^a-z0-9]+', ' ', 'g'),
        '\s+',
        ' ',
        'g'
      )
    ),
    ''
  );
$$;


create or replace function public.fn_prepare_fundraising_for_match()
returns trigger
language plpgsql
security definer
as $$
begin
  new.institution_name_for_match := nullif(btrim(coalesce(new.institution_name, '')), '');
  new.institution_name_norm := public.fn_normalize_institution_name(new.institution_name_for_match);

  if new.institution_id is not null then
    new.institution_match_status := 'matched';
  elsif new.institution_match_status = 'matched' then
    new.institution_match_status := null;
  end if;

  return new;
end;
$$;

drop trigger if exists trg_prepare_fundraising_for_match on public.fundraising;

create trigger trg_prepare_fundraising_for_match
before insert or update of institution_name, institution_id
on public.fundraising
for each row
execute function public.fn_prepare_fundraising_for_match();


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

    if new.review_source = 'fundraising' then
      update public.fundraising f
      set institution_id           = new.alias_institution_id,
          institution_match_status = 'matched'
      where f.institution_id is null
        and f.cw_year              = new.cw_year
        and f.institution_name_norm = new.institution_name_norm;
    else
      update public.signups s
      set institution_id           = new.alias_institution_id,
          institution_match_status = 'matched'
      where s.institution_id is null
        and s.cw_year              = new.cw_year
        and coalesce(s.country_code, '') = new.match_scope_key
        and s.institution_name_norm = new.institution_name_norm;
    end if;

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
      new.match_scope_key,
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
     and new.institution_name_norm is not null
     and coalesce(new.representing_institution, 'Yes') = 'Yes'
  then
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
    values (
      'signup',
      new.cw_year,
      coalesce(new.country_code, ''),
      new.country,
      new.country_code,
      new.institution_name_for_match,
      new.institution_name_norm,
      new.representing_institution,
      new.participation_type,
      1
    )
    on conflict (review_source, cw_year, match_scope_key, institution_name_norm)
    do update
      set n_signups = institution_review.n_signups + 1,
          updated_at = now();
  end if;

  return new;
end;
$$;

create or replace function public.fn_capture_unmatched_fundraising()
returns trigger
language plpgsql
security definer
as $$
begin
  if new.institution_id is null
     and new.institution_name_norm is not null
  then
    insert into public.institution_review (
      review_source,
      cw_year,
      match_scope_key,
      institution_name_for_match,
      institution_name_norm,
      n_signups
    )
    values (
      'fundraising',
      new.cw_year,
      '',
      new.institution_name_for_match,
      new.institution_name_norm,
      1
    )
    on conflict (review_source, cw_year, match_scope_key, institution_name_norm)
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

drop trigger if exists trg_capture_unmatched_fundraising on public.fundraising;
create trigger trg_capture_unmatched_fundraising
after insert on public.fundraising
for each row
execute function public.fn_capture_unmatched_fundraising();



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

  -- 2) Match any unmatched fundraising rows with the same normalised name
  update public.fundraising f
  set institution_id           = new.institution_id,
      institution_match_status = 'matched'
  where f.institution_id is null
    and f.institution_name_norm = new.institution_name_norm;

  -- 3) Resolve any pending institution_review rows for the same name
  update public.institution_review r
  set alias_institution_id = new.institution_id,
      status               = 'resolved',
      updated_at           = now()
  where r.status                = 'pending'
    and r.alias_institution_id  is null
    and r.institution_name_norm = new.institution_name_norm
    and (
      (r.review_source = 'signup' and r.match_scope_key = new.country_code)
      or (r.review_source = 'fundraising' and r.match_scope_key = '')
    );

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

  -- 2) Match any unmatched fundraising rows whose norm name matches this alias
  update public.fundraising f
  set institution_id           = new.institution_id,
      institution_match_status = 'matched'
  where f.institution_id is null
    and f.institution_name_norm = new.alias_name_norm;

  -- 3) Resolve any pending institution_review rows for this alias
  update public.institution_review r
  set alias_institution_id = new.institution_id,
      status               = 'resolved',
      updated_at           = now()
  where r.status                = 'pending'
    and r.alias_institution_id  is null
    and r.institution_name_norm = new.alias_name_norm
    and (
      (r.review_source = 'signup' and r.match_scope_key = new.country_code)
      or (r.review_source = 'fundraising' and r.match_scope_key = '')
    );

  return new;
end;
$$;

drop trigger if exists trg_auto_match_on_alias_insert on public.institution_aliases;

create trigger trg_auto_match_on_alias_insert
after insert on public.institution_aliases
for each row
execute function public.fn_auto_match_on_alias_insert();
