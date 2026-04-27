# CW Institution Matching System

## Overview

This system manages Charity Week (CW) signup data and automatically matches signups to known institutions. The long-term operating model is database-first: new signups are inserted directly into `signups`, and triggers handle unmatched institution capture, admin review, alias creation, and backfilling automatically.

The Python scripts in this repo were used for the initial historical cleanup and backfill of older CSV exports. They are useful as a record of the one-time migration process, but they are not the intended ongoing ingestion path.

When a signup can't be matched, it is queued for admin review. The system supports three resolution paths — all of which propagate matches automatically via database triggers.

---

## Current Operating Model

For normal day-to-day use:

1. New rows are inserted directly into `signups`
2. New fundraising rows are inserted directly into `fundraising`
3. If a row is unmatched, `institution_review` is populated automatically
4. Admins resolve the unmatched name by:
   - linking the review row to an existing institution
   - adding a new institution
   - adding an alias to an existing institution
5. Triggers automatically backfill matching signups or fundraising rows and resolve the review row

The database layer is the product. The review queue, alias propagation, and automatic backfilling all happen inside Postgres/Supabase.

---

## Historical Bootstrap Scripts

These scripts were used to prepare the initial dataset before the trigger-driven workflow was in place:

- `build_institutions.py` builds the initial `institutions.csv`
- `process_signups_2023.py`, `process_signups_2024.py`, and `process_signups_2025.py` clean legacy signup exports and pre-attach `institution_id` where an exact canonical match exists
- `process_sponsors.py` cleans and matches sponsorship data for initial import

These scripts are primarily useful for reproducing the historical import, not for steady-state operations.

---

## Tables

### `institutions`

The master list of all known institutions.

| Column | Type | Description |
|--------|------|-------------|
| `institution_id` | `integer` (identity) | Primary key, auto-generated |
| `country` | `text` | Country name (e.g. `'UK'`) |
| `country_code` | `text` | ISO country code (e.g. `'GB'`) |
| `name_canonical` | `text` | The official/canonical institution name |
| `name_display` | `text` | Display name (can differ from canonical) |
| `institution_name_norm` | `text` | Lowercase normalised name used for matching |
| `legacy_ids` | `jsonb` | Array of IDs from previous systems |
| `aliases` | `jsonb` | Legacy JSONB alias array (deprecated — use `institution_aliases` table) |

**Indexes:** `institution_name_norm`

**Triggers:**
- `trg_auto_match_on_new_institution` — on INSERT, auto-matches waiting signups

---

### `institution_aliases`

Stores alternate names (misspellings, abbreviations, etc.) that map to a known institution. Each alias is scoped to a country.

| Column | Type | Description |
|--------|------|-------------|
| `alias_id` | `bigserial` | Primary key |
| `institution_id` | `integer` | FK → `institutions.institution_id` |
| `country_code` | `text` | ISO country code this alias applies to |
| `alias_name` | `text` | The alternate name as originally written |
| `alias_name_norm` | `text` | Lowercase normalised version, used for matching |
| `created_at` | `timestamptz` | When the alias was added |

**Unique constraint:** `(institution_id, alias_name_norm)` — prevents duplicate aliases per institution.

**Indexes:** `(alias_name_norm, country_code)`

**Triggers:**
- `trg_auto_match_on_alias_insert` — on INSERT, auto-matches unmatched signups and resolves pending reviews

---

### `signups`

Individual signup records submitted each CW year.

| Column | Type | Description |
|--------|------|-------------|
| `signup_id` | `bigint` (identity) | Primary key |
| `cw_year` | `integer` | The Charity Week year (e.g. `2025`) |
| `signup_timestamp` | `timestamptz` | When the signup was submitted |
| `full_name` | `text` | Signup's full name |
| `email` | `text` | Signup's email |
| `representing_institution` | `text` | `'Yes'` / `'No'` / `NULL` (NULL treated as Yes) |
| `participation_type` | `text` | `'University'`, `'Individual'`, etc. |
| `institution_id` | `integer` | FK → `institutions`. NULL if unmatched |
| `institution_match_status` | `text` | `'matched'` or NULL |
| `institution_name_raw` | `text` | Original name as typed by the user |
| `institution_name_for_match` | `text` | Cleaned name used for display/matching |
| `institution_name_norm` | `text` | Lowercase normalised name — the actual matching key |
| `country` / `country_code` | `text` | Signup's country |
| *(other columns)* | | Contact details, head/rep info, geographic fields, etc. |

**Indexes:** `cw_year`, `institution_id`, `(country_code, cw_year)`, `email`

**Triggers:**
- `trg_capture_unmatched_institution` — on INSERT, if `institution_id` is NULL and `representing_institution` ≠ `'No'`, adds/increments a row in `institution_review`

---

### `institution_review`

Shared admin review queue for unmatched institution names from both signups and fundraising. Each row represents a unique `(review_source, cw_year, match_scope_key, institution_name_norm)` combination that couldn't be auto-matched.

| Column | Type | Description |
|--------|------|-------------|
| `review_id` | `bigserial` | Primary key |
| `review_source` | `text` | `'signup'` or `'fundraising'` |
| `cw_year` | `integer` | The CW year |
| `match_scope_key` | `text` | Matching scope key. For signups this is `country_code`; for fundraising it is `''` because fundraising is name-scoped only |
| `country` / `country_code` | `text` | Country of the unmatched signups |
| `institution_name_for_match` | `text` | The name as submitted |
| `institution_name_norm` | `text` | Normalised name |
| `representing_institution` | `text` | From the first signup |
| `participation_type` | `text` | From the first signup |
| `n_signups` | `integer` | Count of queued rows for this unmatched name |
| `alias_institution_id` | `integer` | FK → `institutions`. Set by admin to resolve |
| `status` | `text` | `'pending'` or `'resolved'` |
| `suggestions` | `jsonb` | Optional candidate matches (for UI) |
| `created_at` / `updated_at` | `timestamptz` | Timestamps |

**Unique constraint:** `(review_source, cw_year, match_scope_key, institution_name_norm)` — one review row per source, year, scope, and name.

**Triggers:**
- `trg_resolve_institution_review` (BEFORE UPDATE of `alias_institution_id`) — backfills signups and marks row as resolved
- `trg_resolve_institution_review_alias` (AFTER UPDATE of `alias_institution_id`) — records the alias in `institution_aliases`

---

### `fundraising`

Fundraising rows imported from the fundraising feed. These rows can link to known institutions directly, or enter the shared review workflow when `institution_id` is missing.

| Column | Type | Description |
|--------|------|-------------|
| `sponsor_id` | `bigint` (identity) | Primary key |
| `cw_year` | `integer` | The CW year |
| `community_sponsor_id` | `integer` | External fundraising system ID |
| `institution_name` | `text` | Original institution name from the fundraising row |
| `institution_name_for_match` | `text` | Prepared display value used for matching/review |
| `institution_name_norm` | `text` | Lowercase normalised name used for matching |
| `institution_id` | `integer` | FK → `institutions`. NULL if unmatched |
| `institution_match_status` | `text` | `'matched'` or NULL |
| `region` / `state` | `text` | Geographic context from the fundraising file |
| `launch_good`, `venmo`, `check_amount`, `school_check`, `corporate_match`, `money_order_other`, `total` | `numeric` | Donation amounts by channel |
| `banking_status` | `text` | Banking state of the fundraising line |
| `notes` | `text` | Freeform notes |

**Indexes:** `cw_year`, `institution_id`, `(cw_year, institution_name_norm)`, `banking_status`

**Triggers:**
- `trg_prepare_fundraising_for_match` — on INSERT/UPDATE, prepares the match fields from `institution_name`
- `trg_capture_unmatched_fundraising` — on INSERT, if `institution_id` is NULL, adds/increments a shared review row in `institution_review`

---

## Trigger Functions

### 1. `fn_capture_unmatched_institution()`

**Fires:** AFTER INSERT on `signups`

**Purpose:** Automatically queues unmatched signups for admin review.

**Behaviour:**
- If `institution_id IS NULL` and `representing_institution` is `'Yes'` or `NULL`:
  - Inserts a row into `institution_review` for this `(cw_year, country_code, institution_name_norm)`
  - If that combination already exists, increments `n_signups` by 1
- If `institution_id` is set, or `representing_institution = 'No'`: does nothing

### `fn_capture_unmatched_fundraising()`

**Fires:** AFTER INSERT on `fundraising`

**Purpose:** Automatically queues unmatched fundraising rows for admin review.

**Behaviour:**
- If `institution_id IS NULL` and `institution_name_norm` is present:
   - Inserts a row into `institution_review` with `review_source = 'fundraising'`
   - Uses `match_scope_key = ''`, because fundraising matching is name-scoped only
   - If that combination already exists, increments `n_signups` by 1

---

### 2. `fn_resolve_institution_review()` + `fn_resolve_institution_review_alias()`

**Fires:** BEFORE + AFTER UPDATE of `alias_institution_id` on `institution_review`

**Purpose:** When an admin resolves a review by linking it to a known institution, this propagates the match.

**Behaviour (split across two triggers for technical reasons):**

| Phase | Trigger | Actions |
|-------|---------|---------|
| BEFORE | `fn_resolve_institution_review` | Updates all unmatched source rows matching this review. For signups it uses `(cw_year, country_code, institution_name_norm)`. For fundraising it uses `(cw_year, institution_name_norm)`. Then it sets `status = 'resolved'` on the review row itself. |
| AFTER | `fn_resolve_institution_review_alias` | Inserts the alias into `institution_aliases` (with `ON CONFLICT DO NOTHING`). Signups use their `country_code`; fundraising inserts a global alias with `country_code = ''`. |

> **Why split?** The BEFORE trigger must set `status = 'resolved'` before the row is committed. The alias insert must happen AFTER the row is committed, because the alias insert fires its own trigger (`fn_auto_match_on_alias_insert`), which would otherwise try to update the same review row mid-transaction, causing a conflict.

---

### 3. `fn_auto_match_on_new_institution()`

**Fires:** AFTER INSERT on `institutions`

**Purpose:** When a new institution is added, automatically matches any existing unmatched signups.

**Behaviour:**
- Finds all signups where `institution_id IS NULL` and `country_code` + `institution_name_norm` match the new institution → sets them to matched
- Finds all fundraising rows where `institution_id IS NULL` and `institution_name_norm` matches the new institution → sets them to matched
- Finds all `institution_review` rows with `status = 'pending'` for the same `country_code` + `institution_name_norm` → marks them resolved

---

### 4. `fn_auto_match_on_alias_insert()`

**Fires:** AFTER INSERT on `institution_aliases`

**Purpose:** When a new alias is added, automatically matches unmatched signups that used that alternate name.

**Behaviour:**
- Finds all signups where `institution_id IS NULL` and `country_code` + `institution_name_norm` match the alias → sets them to matched
- Finds all fundraising rows where `institution_id IS NULL` and `institution_name_norm` matches the alias → sets them to matched
- Finds all pending `institution_review` rows for the same alias → marks them resolved

---

## Fundraising Workflow

Fundraising rows now follow the same shared review queue used by signups.

**Steps:**
1. Insert a row into `fundraising`
2. If `institution_id` is already known, the row is treated as matched
3. If `institution_id` is NULL, the row is normalised and queued in `institution_review` with `review_source = 'fundraising'`
4. Admins resolve it by updating `institution_review.alias_institution_id`, or by inserting a new institution or alias
5. Matching fundraising rows are backfilled automatically

**Suggestion policy:**
- Fuzzy suggestions are generated for fundraising review rows
- Auto-accept remains disabled for fundraising; fundraising suggestions are manual-only in the current implementation

---

## Admin Workflows

### Workflow 1: Resolve an Unmatched Name via the Review Queue

This is the most common workflow. Signups arrive with institution names that don't match any known institution.

**Steps:**
1. Query pending reviews:
   ```sql
   SELECT review_id, cw_year, country_code, institution_name_for_match, n_signups
   FROM institution_review
   WHERE status = 'pending'
   ORDER BY n_signups DESC;
   ```
2. Identify the correct institution (e.g. `"Uunveristy of the Arts London"` → institution_id `1336`)
3. Resolve it:
   ```sql
   UPDATE institution_review
   SET alias_institution_id = 1336
   WHERE review_id = 46;
   ```

**What happens automatically:**
- All unmatched signups with that misspelled name (same year + country) get `institution_id = 1336`
- The alias `"uunveristy of the arts london"` is recorded in `institution_aliases`
- The review row status changes to `'resolved'`
- Any future signups with that same misspelling will also be matched (via the alias)

---

### Workflow 2: Add a New Institution

When a completely new institution needs to be added to the system.

**Steps:**
```sql
INSERT INTO institutions (country, country_code, name_canonical, name_display, institution_name_norm)
VALUES ('UK', 'GB', 'University of the Arts London', 'University of the Arts London', 'university of the arts london');
```

**What happens automatically:**
- Any existing unmatched signups with `institution_name_norm = 'university of the arts london'` and `country_code = 'GB'` get matched
- Any pending review rows for that name + country get resolved

---

### Workflow 3: Add an Alias to an Existing Institution

When you discover a misspelling, abbreviation, or alternate name that should map to a known institution.

**Steps:**
```sql
INSERT INTO institution_aliases (institution_id, country_code, alias_name, alias_name_norm)
VALUES (1336, 'GB', 'UAL', 'ual');
```

To add multiple aliases:
```sql
INSERT INTO institution_aliases (institution_id, country_code, alias_name, alias_name_norm)
VALUES
  (1336, 'GB', 'UAL', 'ual'),
  (1336, 'GB', 'Uni of Arts London', 'uni of arts london');
```

**What happens automatically:**
- Any existing unmatched signups with that alias norm name + country get matched
- Any pending review rows for that name + country get resolved

---

## Matching Rules

All matching is based on **two keys**: `institution_name_norm` (lowercase normalised name) and `country_code`.

| Rule | Detail |
|------|--------|
| **Country isolation** | A GB institution will never match a US signup, even if the normalised name is identical |
| **Year independence** | Aliases and institutions match signups across all CW years |
| **No overwrite** | A signup that already has an `institution_id` is never re-matched by any trigger |
| **Deduplication** | Alias inserts use `ON CONFLICT DO NOTHING` — inserting the same alias twice is safe |

---

## System Flow Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                        SIGNUP INSERTED                          │
│                    (institution_id = NULL)                       │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
                 ┌─────────────────────┐
                 │  institution_review  │
                 │  status = 'pending'  │
                 │  n_signups += 1      │
                 └────────┬────────────┘
                          │
          ┌───────────────┼───────────────────┐
          │               │                   │
          ▼               ▼                   ▼
   ┌──────────┐   ┌──────────────┐   ┌───────────────┐
   │ Workflow 1│   │ Workflow 2   │   │ Workflow 3    │
   │ Resolve   │   │ Add new      │   │ Add alias     │
   │ review    │   │ institution  │   │               │
   └─────┬────┘   └──────┬───────┘   └───────┬───────┘
         │               │                   │
         ▼               ▼                   ▼
   ┌──────────────────────────────────────────────┐
   │         signups.institution_id = X           │
   │     signups.institution_match_status =       │
   │                 'matched'                    │
   └──────────────────────────────────────────────┘
```

---

## Testing

A comprehensive SQL test suite is available in `tests.sql` with 14 tests covering all triggers and edge cases. Run the full file in the Supabase SQL Editor — each test cleans up after itself using `cw_year = 9999` as a test marker. If all tests pass, you'll see `ALL TESTS COMPLETE` at the end.

See [tests.sql](tests.sql) for details.
