-- ============================================================
-- TRIGGER TEST SUITE
-- Run in Supabase SQL Editor (one block at a time or all at once)
-- Each test is wrapped in a transaction that ROLLS BACK,
-- so your real data is never touched.
-- ============================================================


-- ============================================================
-- TEST 1: Unmatched signup creates a pending review row
-- Trigger: trg_capture_unmatched_institution
-- ============================================================
do $$
declare
  v_review_count int;
  v_n_signups    int;
  v_status       text;
begin
  raise notice '--- TEST 1: Unmatched signup creates a pending review row ---';

  -- Insert an unmatched signup (no institution_id)
  insert into public.signups (
    cw_year, signup_timestamp, full_name, email,
    representing_institution, participation_type,
    institution_name_for_match, institution_name_norm,
    country, country_code
  ) values (
    9999, now(), 'Test User 1', 'test1@test.com',
    'Yes', 'University',
    'Test University Alpha', 'test university alpha',
    'UK', 'GB'
  );

  -- Verify a review row was created
  select count(*), max(n_signups), max(status)
  into v_review_count, v_n_signups, v_status
  from public.institution_review
  where cw_year = 9999
    and country_code = 'GB'
    and institution_name_norm = 'test university alpha';

  assert v_review_count = 1,
    'FAIL: Expected 1 review row, got ' || v_review_count;
  assert v_n_signups = 1,
    'FAIL: Expected n_signups = 1, got ' || v_n_signups;
  assert v_status = 'pending',
    'FAIL: Expected status = pending, got ' || v_status;

  raise notice 'PASS: Review row created with n_signups=1, status=pending';

  -- Cleanup
  delete from public.institution_review where cw_year = 9999;
  delete from public.signups where cw_year = 9999;
end;
$$;


-- ============================================================
-- TEST 2: Second unmatched signup increments n_signups
-- Trigger: trg_capture_unmatched_institution (ON CONFLICT)
-- ============================================================
do $$
declare
  v_n_signups int;
  v_review_count int;
begin
  raise notice '--- TEST 2: Second signup increments n_signups ---';

  -- Insert two unmatched signups with the same norm name
  insert into public.signups (
    cw_year, signup_timestamp, full_name, email,
    representing_institution, participation_type,
    institution_name_for_match, institution_name_norm,
    country, country_code
  ) values (
    9999, now(), 'Test User 1', 'test1@test.com',
    'Yes', 'University',
    'Test University Beta', 'test university beta',
    'UK', 'GB'
  );

  insert into public.signups (
    cw_year, signup_timestamp, full_name, email,
    representing_institution, participation_type,
    institution_name_for_match, institution_name_norm,
    country, country_code
  ) values (
    9999, now(), 'Test User 2', 'test2@test.com',
    'Yes', 'University',
    'Test University Beta', 'test university beta',
    'UK', 'GB'
  );

  select count(*), max(n_signups)
  into v_review_count, v_n_signups
  from public.institution_review
  where cw_year = 9999
    and country_code = 'GB'
    and institution_name_norm = 'test university beta';

  assert v_review_count = 1,
    'FAIL: Expected 1 review row (upserted), got ' || v_review_count;
  assert v_n_signups = 2,
    'FAIL: Expected n_signups = 2, got ' || v_n_signups;

  raise notice 'PASS: Single review row with n_signups=2';

  delete from public.institution_review where cw_year = 9999;
  delete from public.signups where cw_year = 9999;
end;
$$;


-- ============================================================
-- TEST 3: Matched signup does NOT create a review row
-- Trigger: trg_capture_unmatched_institution (should skip)
-- ============================================================
do $$
declare
  v_inst_id int;
  v_review_count int;
begin
  raise notice '--- TEST 3: Matched signup does not create review row ---';

  -- Create a real institution first (explicit high ID to avoid sequence collisions)
  insert into public.institutions (institution_id, country, country_code, name_canonical, institution_name_norm)
  values (999001, 'UK', 'GB', 'Test Matched Uni', 'test matched uni')
  returning institution_id into v_inst_id;

  -- Insert a signup that IS matched (has institution_id)
  insert into public.signups (
    cw_year, signup_timestamp, full_name, email,
    representing_institution, participation_type,
    institution_id, institution_match_status,
    institution_name_for_match, institution_name_norm,
    country, country_code
  ) values (
    9999, now(), 'Test User 3', 'test3@test.com',
    'Yes', 'University',
    v_inst_id, 'matched',
    'Test Matched Uni', 'test matched uni',
    'UK', 'GB'
  );

  select count(*) into v_review_count
  from public.institution_review
  where cw_year = 9999
    and institution_name_norm = 'test matched uni';

  assert v_review_count = 0,
    'FAIL: Should not create review row for matched signup, got ' || v_review_count;

  raise notice 'PASS: No review row created for matched signup';

  delete from public.signups where cw_year = 9999;
  delete from public.institution_aliases where institution_id = v_inst_id;
  delete from public.institutions where institution_id = v_inst_id;
end;
$$;


-- ============================================================
-- TEST 4: representing_institution = 'No' does NOT create review
-- Trigger: trg_capture_unmatched_institution (should skip)
-- ============================================================
do $$
declare
  v_review_count int;
begin
  raise notice '--- TEST 4: representing_institution=No skips review ---';

  insert into public.signups (
    cw_year, signup_timestamp, full_name, email,
    representing_institution, participation_type,
    institution_name_for_match, institution_name_norm,
    country, country_code
  ) values (
    9999, now(), 'Individual User', 'indiv@test.com',
    'No', 'Individual',
    'Some Institution', 'some institution',
    'UK', 'GB'
  );

  select count(*) into v_review_count
  from public.institution_review
  where cw_year = 9999
    and institution_name_norm = 'some institution';

  assert v_review_count = 0,
    'FAIL: Should not create review for representing_institution=No, got ' || v_review_count;

  raise notice 'PASS: No review row when representing_institution=No';

  delete from public.signups where cw_year = 9999;
end;
$$;


-- ============================================================
-- TEST 5: New institution auto-matches existing unmatched signups
-- Trigger: trg_auto_match_on_new_institution
-- ============================================================
do $$
declare
  v_inst_id int;
  v_signup_inst_id int;
  v_match_status text;
  v_review_status text;
begin
  raise notice '--- TEST 5: New institution auto-matches waiting signups ---';

  -- 1) Insert an unmatched signup (creates a review row too)
  insert into public.signups (
    cw_year, signup_timestamp, full_name, email,
    representing_institution, participation_type,
    institution_name_for_match, institution_name_norm,
    country, country_code
  ) values (
    9999, now(), 'Waiting User', 'waiting@test.com',
    'Yes', 'University',
    'Brand New University', 'brand new university',
    'UK', 'GB'
  );

  -- 2) Now an admin adds that institution → trigger should backfill
  insert into public.institutions (institution_id, country, country_code, name_canonical, institution_name_norm)
  values (999002, 'UK', 'GB', 'Brand New University', 'brand new university')
  returning institution_id into v_inst_id;

  -- 3) Check the signup got matched
  select institution_id, institution_match_status
  into v_signup_inst_id, v_match_status
  from public.signups
  where cw_year = 9999 and email = 'waiting@test.com';

  assert v_signup_inst_id = v_inst_id,
    'FAIL: Signup institution_id should be ' || v_inst_id || ', got ' || coalesce(v_signup_inst_id::text, 'NULL');
  assert v_match_status = 'matched',
    'FAIL: Signup should be matched, got ' || coalesce(v_match_status, 'NULL');

  -- 4) Check the review row got resolved
  select status into v_review_status
  from public.institution_review
  where cw_year = 9999
    and country_code = 'GB'
    and institution_name_norm = 'brand new university';

  assert v_review_status = 'resolved',
    'FAIL: Review should be resolved, got ' || coalesce(v_review_status, 'NULL');

  raise notice 'PASS: Signup matched and review resolved on institution insert';

  delete from public.signups where cw_year = 9999;
  delete from public.institution_review where cw_year = 9999;
  delete from public.institution_aliases where institution_id = v_inst_id;
  delete from public.institutions where institution_id = v_inst_id;
end;
$$;


-- ============================================================
-- TEST 6: Alias insert auto-matches unmatched signups
-- Trigger: trg_auto_match_on_alias_insert
-- ============================================================
do $$
declare
  v_inst_id int;
  v_signup_inst_id int;
  v_match_status text;
  v_review_status text;
begin
  raise notice '--- TEST 6: Alias insert auto-matches signups ---';

  -- 1) Create an institution with its canonical name
  insert into public.institutions (institution_id, country, country_code, name_canonical, institution_name_norm)
  values (999003, 'UK', 'GB', 'University of the Arts London', 'university of the arts london')
  returning institution_id into v_inst_id;

  -- 2) Insert a signup with a MISSPELLED name (won't match the institution)
  insert into public.signups (
    cw_year, signup_timestamp, full_name, email,
    representing_institution, participation_type,
    institution_name_for_match, institution_name_norm,
    country, country_code
  ) values (
    9999, now(), 'Typo User', 'typo@test.com',
    'Yes', 'University',
    'Uunveristy of the Arts London', 'uunveristy of the arts london',
    'UK', 'GB'
  );

  -- 3) Admin adds the misspelling as an alias → trigger should match
  insert into public.institution_aliases (institution_id, country_code, alias_name, alias_name_norm)
  values (v_inst_id, 'GB', 'Uunveristy of the Arts London', 'uunveristy of the arts london');

  -- 4) Check signup got matched
  select institution_id, institution_match_status
  into v_signup_inst_id, v_match_status
  from public.signups
  where cw_year = 9999 and email = 'typo@test.com';

  assert v_signup_inst_id = v_inst_id,
    'FAIL: Signup should be matched to ' || v_inst_id || ', got ' || coalesce(v_signup_inst_id::text, 'NULL');
  assert v_match_status = 'matched',
    'FAIL: Signup status should be matched, got ' || coalesce(v_match_status, 'NULL');

  -- 5) Check review row got resolved
  select status into v_review_status
  from public.institution_review
  where cw_year = 9999
    and country_code = 'GB'
    and institution_name_norm = 'uunveristy of the arts london';

  assert v_review_status = 'resolved',
    'FAIL: Review should be resolved, got ' || coalesce(v_review_status, 'NULL');

  raise notice 'PASS: Alias insert matched signup and resolved review';

  delete from public.signups where cw_year = 9999;
  delete from public.institution_review where cw_year = 9999;
  delete from public.institution_aliases where institution_id = v_inst_id;
  delete from public.institutions where institution_id = v_inst_id;
end;
$$;


-- ============================================================
-- TEST 7: Resolving a review backfills signups + creates alias
-- Trigger: trg_resolve_institution_review
-- ============================================================
do $$
declare
  v_inst_id int;
  v_review_id bigint;
  v_signup_inst_id int;
  v_match_status text;
  v_alias_count int;
  v_review_status text;
begin
  raise notice '--- TEST 7: Resolving review backfills signups + creates alias ---';

  -- 1) Create a known institution
  insert into public.institutions (institution_id, country, country_code, name_canonical, institution_name_norm)
  values (999004, 'UK', 'GB', 'Correct Name Uni', 'correct name uni')
  returning institution_id into v_inst_id;

  -- 2) Insert unmatched signups (creates review row via trigger)
  insert into public.signups (
    cw_year, signup_timestamp, full_name, email,
    representing_institution, participation_type,
    institution_name_for_match, institution_name_norm,
    country, country_code
  ) values (
    9999, now(), 'Review User 1', 'rev1@test.com',
    'Yes', 'University',
    'Correkt Name Uni', 'correkt name uni',
    'UK', 'GB'
  );

  insert into public.signups (
    cw_year, signup_timestamp, full_name, email,
    representing_institution, participation_type,
    institution_name_for_match, institution_name_norm,
    country, country_code
  ) values (
    9999, now(), 'Review User 2', 'rev2@test.com',
    'Yes', 'University',
    'Correkt Name Uni', 'correkt name uni',
    'UK', 'GB'
  );

  -- 3) Get the review_id
  select review_id into v_review_id
  from public.institution_review
  where cw_year = 9999
    and country_code = 'GB'
    and institution_name_norm = 'correkt name uni';

  assert v_review_id is not null, 'FAIL: Review row should exist';

  -- 4) Admin resolves the review by setting alias_institution_id
  update public.institution_review
  set alias_institution_id = v_inst_id
  where review_id = v_review_id;

  -- 5) Check both signups got matched
  select count(*) into v_signup_inst_id
  from public.signups
  where cw_year = 9999
    and institution_name_norm = 'correkt name uni'
    and institution_id = v_inst_id
    and institution_match_status = 'matched';

  assert v_signup_inst_id = 2,
    'FAIL: Both signups should be matched, got ' || v_signup_inst_id;

  -- 6) Check alias was created in institution_aliases
  select count(*) into v_alias_count
  from public.institution_aliases
  where institution_id = v_inst_id
    and alias_name_norm = 'correkt name uni';

  assert v_alias_count = 1,
    'FAIL: Alias should be created, got ' || v_alias_count;

  -- 7) Check review status
  select status into v_review_status
  from public.institution_review
  where review_id = v_review_id;

  assert v_review_status = 'resolved',
    'FAIL: Review should be resolved, got ' || coalesce(v_review_status, 'NULL');

  raise notice 'PASS: Review resolution backfilled 2 signups and created alias';

  delete from public.signups where cw_year = 9999;
  delete from public.institution_review where cw_year = 9999;
  delete from public.institution_aliases where institution_id = v_inst_id;
  delete from public.institutions where institution_id = v_inst_id;
end;
$$;


-- ============================================================
-- TEST 8: EDGE CASE — Same norm name, different country codes
-- Should NOT cross-match between countries
-- ============================================================
do $$
declare
  v_inst_id_gb int;
  v_signup_us_inst_id int;
begin
  raise notice '--- TEST 8: Different country codes do not cross-match ---';

  -- 1) Create a GB institution
  insert into public.institutions (institution_id, country, country_code, name_canonical, institution_name_norm)
  values (999005, 'UK', 'GB', 'National University', 'national university')
  returning institution_id into v_inst_id_gb;

  -- 2) Insert a US signup with the same norm name
  insert into public.signups (
    cw_year, signup_timestamp, full_name, email,
    representing_institution, participation_type,
    institution_name_for_match, institution_name_norm,
    country, country_code
  ) values (
    9999, now(), 'US User', 'us@test.com',
    'Yes', 'University',
    'National University', 'national university',
    'US', 'US'
  );

  -- 3) The US signup should NOT have been matched to the GB institution
  select institution_id into v_signup_us_inst_id
  from public.signups
  where cw_year = 9999 and email = 'us@test.com';

  assert v_signup_us_inst_id is null,
    'FAIL: US signup should NOT match GB institution, got inst_id=' || v_signup_us_inst_id;

  raise notice 'PASS: Country isolation working — US signup unmatched';

  delete from public.signups where cw_year = 9999;
  delete from public.institution_review where cw_year = 9999;
  delete from public.institution_aliases where institution_id = v_inst_id_gb;
  delete from public.institutions where institution_id = v_inst_id_gb;
end;
$$;


-- ============================================================
-- TEST 9: EDGE CASE — Same norm name, different CW years
-- Signups from other years should still match on institution insert
-- (fn_auto_match_on_new_institution does NOT filter by year)
-- ============================================================
do $$
declare
  v_inst_id int;
  v_matched_count int;
begin
  raise notice '--- TEST 9: Signups from multiple years match on institution insert ---';

  -- 1) Insert unmatched signups across two years
  insert into public.signups (
    cw_year, signup_timestamp, full_name, email,
    representing_institution, participation_type,
    institution_name_for_match, institution_name_norm,
    country, country_code
  ) values
    (9998, now(), 'Year1 User', 'y1@test.com', 'Yes', 'University',
     'Multi Year Uni', 'multi year uni', 'UK', 'GB'),
    (9999, now(), 'Year2 User', 'y2@test.com', 'Yes', 'University',
     'Multi Year Uni', 'multi year uni', 'UK', 'GB');

  -- 2) Insert the institution
  insert into public.institutions (institution_id, country, country_code, name_canonical, institution_name_norm)
  values (999006, 'UK', 'GB', 'Multi Year Uni', 'multi year uni')
  returning institution_id into v_inst_id;

  -- 3) Both signups should be matched
  select count(*) into v_matched_count
  from public.signups
  where institution_id = v_inst_id
    and institution_match_status = 'matched';

  assert v_matched_count = 2,
    'FAIL: Both year signups should be matched, got ' || v_matched_count;

  raise notice 'PASS: Cross-year signups matched on institution insert';

  delete from public.signups where cw_year in (9998, 9999);
  delete from public.institution_review where cw_year in (9998, 9999);
  delete from public.institution_aliases where institution_id = v_inst_id;
  delete from public.institutions where institution_id = v_inst_id;
end;
$$;


-- ============================================================
-- TEST 10: EDGE CASE — Already-matched signup is NOT re-matched
-- A signup that already has an institution_id should be left alone
-- ============================================================
do $$
declare
  v_inst_id_a int;
  v_inst_id_b int;
  v_signup_inst_id int;
begin
  raise notice '--- TEST 10: Already-matched signups are not overwritten ---';

  -- 1) Create two institutions
  insert into public.institutions (institution_id, country, country_code, name_canonical, institution_name_norm)
  values (999007, 'UK', 'GB', 'Original Uni', 'original uni')
  returning institution_id into v_inst_id_a;

  insert into public.institutions (institution_id, country, country_code, name_canonical, institution_name_norm)
  values (999008, 'UK', 'GB', 'Duplicate Uni', 'duplicate uni')
  returning institution_id into v_inst_id_b;

  -- 2) Insert a signup already matched to institution A
  insert into public.signups (
    cw_year, signup_timestamp, full_name, email,
    representing_institution, participation_type,
    institution_id, institution_match_status,
    institution_name_for_match, institution_name_norm,
    country, country_code
  ) values (
    9999, now(), 'Stable User', 'stable@test.com',
    'Yes', 'University',
    v_inst_id_a, 'matched',
    'Duplicate Uni', 'duplicate uni',
    'UK', 'GB'
  );

  -- 3) Add an alias that matches this signup's norm name → should NOT override
  insert into public.institution_aliases (institution_id, country_code, alias_name, alias_name_norm)
  values (v_inst_id_b, 'GB', 'Duplicate Uni', 'duplicate uni');

  -- 4) Signup should still be matched to institution A
  select institution_id into v_signup_inst_id
  from public.signups
  where cw_year = 9999 and email = 'stable@test.com';

  assert v_signup_inst_id = v_inst_id_a,
    'FAIL: Signup should still be with inst A (' || v_inst_id_a || '), got ' || v_signup_inst_id;

  raise notice 'PASS: Already-matched signup was not overwritten';

  delete from public.signups where cw_year = 9999;
  delete from public.institution_review where cw_year = 9999;
  delete from public.institution_aliases where institution_id in (v_inst_id_a, v_inst_id_b);
  delete from public.institutions where institution_id in (v_inst_id_a, v_inst_id_b);
end;
$$;


-- ============================================================
-- TEST 11: EDGE CASE — Duplicate alias insert (ON CONFLICT)
-- Inserting the same alias twice should not error
-- ============================================================
do $$
declare
  v_inst_id int;
  v_alias_count int;
begin
  raise notice '--- TEST 11: Duplicate alias insert does not error ---';

  insert into public.institutions (institution_id, country, country_code, name_canonical, institution_name_norm)
  values (999009, 'UK', 'GB', 'Dedup Uni', 'dedup uni')
  returning institution_id into v_inst_id;

  -- Insert alias twice
  insert into public.institution_aliases (institution_id, country_code, alias_name, alias_name_norm)
  values (v_inst_id, 'GB', 'Dedup Alias', 'dedup alias');

  -- Second insert should conflict — this will raise unless handled
  begin
    insert into public.institution_aliases (institution_id, country_code, alias_name, alias_name_norm)
    values (v_inst_id, 'GB', 'Dedup Alias', 'dedup alias');
    raise notice 'NOTE: Second insert succeeded (no constraint error)';
  exception when unique_violation then
    raise notice 'NOTE: Second insert raised unique_violation (expected)';
  end;

  select count(*) into v_alias_count
  from public.institution_aliases
  where institution_id = v_inst_id and alias_name_norm = 'dedup alias';

  assert v_alias_count = 1,
    'FAIL: Should have exactly 1 alias, got ' || v_alias_count;

  raise notice 'PASS: Duplicate alias handled correctly';

  delete from public.institution_aliases where institution_id = v_inst_id;
  delete from public.institutions where institution_id = v_inst_id;
end;
$$;


-- ============================================================
-- TEST 12: EDGE CASE — Review resolution creates alias, which
-- triggers fn_auto_match_on_alias_insert. Verify no double-fire
-- issues or conflicts.
-- (Chain: review resolved → alias inserted → alias trigger fires)
-- ============================================================
do $$
declare
  v_inst_id int;
  v_review_id bigint;
  v_matched_count int;
  v_alias_count int;
begin
  raise notice '--- TEST 12: Review resolve → alias insert chain ---';

  -- 1) Institution exists
  insert into public.institutions (institution_id, country, country_code, name_canonical, institution_name_norm)
  values (999010, 'UK', 'GB', 'Chain Uni', 'chain uni')
  returning institution_id into v_inst_id;

  -- 2) Three unmatched signups with a typo name
  insert into public.signups (
    cw_year, signup_timestamp, full_name, email,
    representing_institution, participation_type,
    institution_name_for_match, institution_name_norm,
    country, country_code
  ) values
    (9999, now(), 'Chain User 1', 'chain1@test.com', 'Yes', 'University',
     'Chayn Uni', 'chayn uni', 'UK', 'GB'),
    (9999, now(), 'Chain User 2', 'chain2@test.com', 'Yes', 'University',
     'Chayn Uni', 'chayn uni', 'UK', 'GB'),
    (9999, now(), 'Chain User 3', 'chain3@test.com', 'Yes', 'University',
     'Chayn Uni', 'chayn uni', 'UK', 'GB');

  -- 3) Get review row
  select review_id into v_review_id
  from public.institution_review
  where cw_year = 9999
    and country_code = 'GB'
    and institution_name_norm = 'chayn uni';

  -- 4) Resolve it — this should:
  --    a) Insert into institution_aliases (fn_resolve_institution_review)
  --    b) Which fires fn_auto_match_on_alias_insert
  --    c) fn_resolve also directly updates signups
  update public.institution_review
  set alias_institution_id = v_inst_id
  where review_id = v_review_id;

  -- 5) All 3 signups should be matched (no errors from double-update)
  select count(*) into v_matched_count
  from public.signups
  where cw_year = 9999
    and institution_name_norm = 'chayn uni'
    and institution_id = v_inst_id
    and institution_match_status = 'matched';

  assert v_matched_count = 3,
    'FAIL: All 3 signups should be matched, got ' || v_matched_count;

  -- 6) Exactly 1 alias should exist (no duplicates from chain)
  select count(*) into v_alias_count
  from public.institution_aliases
  where institution_id = v_inst_id
    and alias_name_norm = 'chayn uni';

  assert v_alias_count = 1,
    'FAIL: Should have exactly 1 alias, got ' || v_alias_count;

  raise notice 'PASS: Chain trigger executed without conflicts';

  delete from public.signups where cw_year = 9999;
  delete from public.institution_review where cw_year = 9999;
  delete from public.institution_aliases where institution_id = v_inst_id;
  delete from public.institutions where institution_id = v_inst_id;
end;
$$;


-- ============================================================
-- TEST 13: EDGE CASE — NULL representing_institution defaults
-- to 'Yes' (per COALESCE logic)
-- ============================================================
do $$
declare
  v_review_count int;
begin
  raise notice '--- TEST 13: NULL representing_institution treated as Yes ---';

  insert into public.signups (
    cw_year, signup_timestamp, full_name, email,
    representing_institution, participation_type,
    institution_name_for_match, institution_name_norm,
    country, country_code
  ) values (
    9999, now(), 'Null Rep User', 'nullrep@test.com',
    null, 'University',
    'Null Rep Uni', 'null rep uni',
    'UK', 'GB'
  );

  select count(*) into v_review_count
  from public.institution_review
  where cw_year = 9999
    and institution_name_norm = 'null rep uni';

  assert v_review_count = 1,
    'FAIL: NULL representing_institution should create review, got ' || v_review_count;

  raise notice 'PASS: NULL representing_institution creates review row';

  delete from public.institution_review where cw_year = 9999;
  delete from public.signups where cw_year = 9999;
end;
$$;


-- ============================================================
-- TEST 14: EDGE CASE — Alias for country A should not match
-- signups in country B (even with same norm name)
-- ============================================================
do $$
declare
  v_inst_id int;
  v_gb_inst_id int;
  v_us_inst_id int;
begin
  raise notice '--- TEST 14: Alias country isolation ---';

  -- GB institution
  insert into public.institutions (institution_id, country, country_code, name_canonical, institution_name_norm)
  values (999011, 'UK', 'GB', 'Crossover Uni GB', 'crossover uni')
  returning institution_id into v_gb_inst_id;

  -- US signup with same norm name
  insert into public.signups (
    cw_year, signup_timestamp, full_name, email,
    representing_institution, participation_type,
    institution_name_for_match, institution_name_norm,
    country, country_code
  ) values (
    9999, now(), 'US Crossover', 'uscross@test.com',
    'Yes', 'University',
    'Crossover Uni', 'crossover uni',
    'US', 'US'
  );

  -- Add GB alias
  insert into public.institution_aliases (institution_id, country_code, alias_name, alias_name_norm)
  values (v_gb_inst_id, 'GB', 'Crossover Alias', 'crossover uni');

  -- US signup should still be unmatched
  select institution_id into v_us_inst_id
  from public.signups
  where cw_year = 9999 and email = 'uscross@test.com';

  assert v_us_inst_id is null,
    'FAIL: US signup should not match GB alias, got ' || v_us_inst_id;

  raise notice 'PASS: Alias country isolation verified';

  delete from public.signups where cw_year = 9999;
  delete from public.institution_review where cw_year = 9999;
  delete from public.institution_aliases where institution_id = v_gb_inst_id;
  delete from public.institutions where institution_id = v_gb_inst_id;
end;
$$;


-- ============================================================
-- TEST 15: Unmatched fundraising row creates a pending review row
-- Trigger: trg_prepare_fundraising_for_match + trg_capture_unmatched_fundraising
-- ============================================================
do $$
declare
  v_review_count int;
  v_n_signups int;
  v_status text;
  v_name_for_match text;
  v_name_norm text;
begin
  raise notice '--- TEST 15: Unmatched fundraising row creates a pending review row ---';

  insert into public.fundraising (
    cw_year,
    community_sponsor_id,
    institution_name
  ) values (
    9999,
    99001,
    'Test Fundraising University'
  );

  select institution_name_for_match, institution_name_norm
  into v_name_for_match, v_name_norm
  from public.fundraising
  where cw_year = 9999
    and community_sponsor_id = 99001;

  assert v_name_for_match = 'Test Fundraising University',
    'FAIL: Expected institution_name_for_match to be prepared from institution_name';
  assert v_name_norm = 'test fundraising university',
    'FAIL: Expected institution_name_norm to be normalized, got ' || coalesce(v_name_norm, 'NULL');

  select count(*), max(n_signups), max(status)
  into v_review_count, v_n_signups, v_status
  from public.institution_review
  where review_source = 'fundraising'
    and cw_year = 9999
    and match_scope_key = ''
    and institution_name_norm = 'test fundraising university';

  assert v_review_count = 1,
    'FAIL: Expected 1 fundraising review row, got ' || v_review_count;
  assert v_n_signups = 1,
    'FAIL: Expected n_signups = 1, got ' || v_n_signups;
  assert v_status = 'pending',
    'FAIL: Expected status = pending, got ' || coalesce(v_status, 'NULL');

  raise notice 'PASS: Fundraising row normalized and queued for review';

  delete from public.institution_review where cw_year = 9999 and review_source = 'fundraising';
  delete from public.fundraising where cw_year = 9999;
end;
$$;


-- ============================================================
-- TEST 16: Second unmatched fundraising row increments n_signups
-- Trigger: trg_capture_unmatched_fundraising (ON CONFLICT)
-- ============================================================
do $$
declare
  v_review_count int;
  v_n_signups int;
begin
  raise notice '--- TEST 16: Duplicate fundraising name increments review count ---';

  insert into public.fundraising (cw_year, community_sponsor_id, institution_name)
  values
    (9999, 99002, 'Fundraising Queue Uni'),
    (9999, 99003, 'Fundraising Queue Uni');

  select count(*), max(n_signups)
  into v_review_count, v_n_signups
  from public.institution_review
  where review_source = 'fundraising'
    and cw_year = 9999
    and match_scope_key = ''
    and institution_name_norm = 'fundraising queue uni';

  assert v_review_count = 1,
    'FAIL: Expected 1 fundraising review row, got ' || v_review_count;
  assert v_n_signups = 2,
    'FAIL: Expected n_signups = 2, got ' || v_n_signups;

  raise notice 'PASS: Fundraising review row reused and incremented';

  delete from public.institution_review where cw_year = 9999 and review_source = 'fundraising';
  delete from public.fundraising where cw_year = 9999;
end;
$$;


-- ============================================================
-- TEST 17: Resolving a fundraising review backfills rows + creates alias
-- Trigger: trg_resolve_institution_review + trg_resolve_institution_review_alias
-- ============================================================
do $$
declare
  v_inst_id int;
  v_review_id bigint;
  v_matched_count int;
  v_alias_count int;
  v_review_status text;
begin
  raise notice '--- TEST 17: Resolving fundraising review backfills rows + creates alias ---';

  insert into public.institutions (institution_id, country, country_code, name_canonical, institution_name_norm)
  values (999012, 'USA', 'US', 'Fundraising Correct Uni', 'fundraising correct uni')
  returning institution_id into v_inst_id;

  insert into public.fundraising (cw_year, community_sponsor_id, institution_name)
  values
    (9999, 99004, 'Fundrasing Correct Uni'),
    (9999, 99005, 'Fundrasing Correct Uni');

  select review_id into v_review_id
  from public.institution_review
  where review_source = 'fundraising'
    and cw_year = 9999
    and institution_name_norm = 'fundrasing correct uni';

  assert v_review_id is not null, 'FAIL: Fundraising review row should exist';

  update public.institution_review
  set alias_institution_id = v_inst_id
  where review_id = v_review_id;

  select count(*) into v_matched_count
  from public.fundraising
  where cw_year = 9999
    and institution_name_norm = 'fundrasing correct uni'
    and institution_id = v_inst_id
    and institution_match_status = 'matched';

  assert v_matched_count = 2,
    'FAIL: Both fundraising rows should be matched, got ' || v_matched_count;

  select count(*) into v_alias_count
  from public.institution_aliases
  where institution_id = v_inst_id
    and alias_name_norm = 'fundrasing correct uni'
    and country_code = '';

  assert v_alias_count = 1,
    'FAIL: Expected 1 global alias for fundraising review, got ' || v_alias_count;

  select status into v_review_status
  from public.institution_review
  where review_id = v_review_id;

  assert v_review_status = 'resolved',
    'FAIL: Fundraising review should be resolved, got ' || coalesce(v_review_status, 'NULL');

  raise notice 'PASS: Fundraising review resolution backfilled rows and inserted alias';

  delete from public.fundraising where cw_year = 9999;
  delete from public.institution_review where cw_year = 9999 and review_source = 'fundraising';
  delete from public.institution_aliases where institution_id = v_inst_id;
  delete from public.institutions where institution_id = v_inst_id;
end;
$$;


-- ============================================================
-- TEST 18: New institution auto-matches existing unmatched fundraising rows
-- Trigger: trg_auto_match_on_new_institution
-- ============================================================
do $$
declare
  v_inst_id int;
  v_fundraising_inst_id int;
  v_match_status text;
  v_review_status text;
begin
  raise notice '--- TEST 18: New institution auto-matches fundraising rows ---';

  insert into public.fundraising (cw_year, community_sponsor_id, institution_name)
  values (9999, 99006, 'Fresh Fundraising Uni');

  insert into public.institutions (institution_id, country, country_code, name_canonical, institution_name_norm)
  values (999013, 'USA', 'US', 'Fresh Fundraising Uni', 'fresh fundraising uni')
  returning institution_id into v_inst_id;

  select institution_id, institution_match_status
  into v_fundraising_inst_id, v_match_status
  from public.fundraising
  where cw_year = 9999
    and community_sponsor_id = 99006;

  assert v_fundraising_inst_id = v_inst_id,
    'FAIL: Fundraising row should be matched to new institution';
  assert v_match_status = 'matched',
    'FAIL: Fundraising row should be marked matched, got ' || coalesce(v_match_status, 'NULL');

  select status into v_review_status
  from public.institution_review
  where review_source = 'fundraising'
    and cw_year = 9999
    and institution_name_norm = 'fresh fundraising uni';

  assert v_review_status = 'resolved',
    'FAIL: Fundraising review should be resolved, got ' || coalesce(v_review_status, 'NULL');

  raise notice 'PASS: Institution insert matched fundraising row and resolved review';

  delete from public.fundraising where cw_year = 9999;
  delete from public.institution_review where cw_year = 9999 and review_source = 'fundraising';
  delete from public.institution_aliases where institution_id = v_inst_id;
  delete from public.institutions where institution_id = v_inst_id;
end;
$$;


-- ============================================================
-- TEST 19: Alias insert auto-matches unmatched fundraising rows
-- Trigger: trg_auto_match_on_alias_insert
-- ============================================================
do $$
declare
  v_inst_id int;
  v_fundraising_inst_id int;
  v_match_status text;
  v_review_status text;
begin
  raise notice '--- TEST 19: Alias insert auto-matches fundraising rows ---';

  insert into public.institutions (institution_id, country, country_code, name_canonical, institution_name_norm)
  values (999014, 'USA', 'US', 'Fundraising Alias Uni', 'fundraising alias uni')
  returning institution_id into v_inst_id;

  insert into public.fundraising (cw_year, community_sponsor_id, institution_name)
  values (9999, 99007, 'Fundrasing Alias Uni');

  insert into public.institution_aliases (institution_id, country_code, alias_name, alias_name_norm)
  values (v_inst_id, 'US', 'Fundrasing Alias Uni', 'fundrasing alias uni');

  select institution_id, institution_match_status
  into v_fundraising_inst_id, v_match_status
  from public.fundraising
  where cw_year = 9999
    and community_sponsor_id = 99007;

  assert v_fundraising_inst_id = v_inst_id,
    'FAIL: Fundraising row should be matched via alias';
  assert v_match_status = 'matched',
    'FAIL: Fundraising row should be marked matched, got ' || coalesce(v_match_status, 'NULL');

  select status into v_review_status
  from public.institution_review
  where review_source = 'fundraising'
    and cw_year = 9999
    and institution_name_norm = 'fundrasing alias uni';

  assert v_review_status = 'resolved',
    'FAIL: Fundraising review should be resolved, got ' || coalesce(v_review_status, 'NULL');

  raise notice 'PASS: Alias insert matched fundraising row and resolved review';

  delete from public.fundraising where cw_year = 9999;
  delete from public.institution_review where cw_year = 9999 and review_source = 'fundraising';
  delete from public.institution_aliases where institution_id = v_inst_id;
  delete from public.institutions where institution_id = v_inst_id;
end;
$$;


-- ============================================================
-- TEST 20: Auto-accept suggestions does not auto-resolve fundraising reviews
-- Function: fn_auto_accept_suggestions
-- ============================================================
do $$
declare
  v_inst_id int;
  v_status text;
  v_alias_inst_id int;
begin
  raise notice '--- TEST 20: Fundraising suggestions stay manual-only ---';

  insert into public.institutions (institution_id, country, country_code, name_canonical, institution_name_norm)
  values (999015, 'USA', 'US', 'Manual Suggestion Uni', 'manual suggestion uni')
  returning institution_id into v_inst_id;

  insert into public.institution_review (
    review_source,
    cw_year,
    match_scope_key,
    institution_name_for_match,
    institution_name_norm,
    n_signups,
    suggested_institution_id,
    suggested_institution_name,
    confidence
  ) values (
    'fundraising',
    9999,
    '',
    'Manual Suggestion Uni',
    'manual suggestion uni',
    1,
    v_inst_id,
    'Manual Suggestion Uni',
    100
  );

  perform public.fn_auto_accept_suggestions();

  select status, alias_institution_id
  into v_status, v_alias_inst_id
  from public.institution_review
  where review_source = 'fundraising'
    and cw_year = 9999
    and institution_name_norm = 'manual suggestion uni';

  assert v_status = 'pending',
    'FAIL: Fundraising review should remain pending, got ' || coalesce(v_status, 'NULL');
  assert v_alias_inst_id is null,
    'FAIL: Fundraising review should not be auto-accepted';

  raise notice 'PASS: Fundraising suggestions remain manual-only';

  delete from public.institution_review where cw_year = 9999 and review_source = 'fundraising';
  delete from public.institution_aliases where institution_id = v_inst_id;
  delete from public.institutions where institution_id = v_inst_id;
end;
$$;


-- ============================================================
-- SUMMARY
-- ============================================================
do $$
begin
  raise notice '';
  raise notice '============================================';
  raise notice ' ALL TESTS COMPLETE';
  raise notice ' If you see this, every assertion passed.';
  raise notice '============================================';
end;
$$;
