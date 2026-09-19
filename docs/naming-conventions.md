# Naming conventions

Consistent names are the cheapest governance control there is. They make lineage readable, permissions predictable, and automation possible.

## Workspaces

Pattern: `ws_<domain>_<env>`, lowercase, **no spaces** (spaces make OneLake `abfss://` paths awkward and break some tooling).

| Example | Purpose |
|---|---|
| `ws_snf_dev` | Development for the skilled nursing data domain |
| `ws_snf_test` | Pre-production validation |
| `ws_snf_prod` | Production; deployed to, never edited directly |

## Items

| Item | Pattern | Example |
|---|---|---|
| Lakehouse | `lh_<layer>` | `lh_bronze`, `lh_silver`, `lh_gold` |
| Notebook | `nb_<nn>_<layer>_<action>` | `nb_01_bronze_ingest` |
| Data pipeline | `pl_<domain>_<purpose>` | `pl_snf_daily_load` |
| Semantic model | `sm_<domain>_<subject>` | `sm_snf_census_ar` |
| Report | `rpt_<audience>_<subject>` | `rpt_exec_ar_summary` |

## Tables

| Layer | Pattern | Example |
|---|---|---|
| Bronze | Source name, unchanged | `fact_invoice` |
| Silver | Same as source, plus `<table>_quarantine` and `dq_results` | `fact_invoice_quarantine` |
| Gold | `dim_<noun>`, `fct_<noun>`, `fct_<noun>_snapshot` | `dim_facility`, `fct_census_daily`, `fct_ar_aging_snapshot` |

## Columns

- `snake_case`, singular nouns, no spaces or reserved words.
- Keys end in `_id` (business key) or `_key` (surrogate key).
- Dates end in `_date`, timestamps in `_ts`, amounts in `_amount`, counts in `_count`, flags start with `is_`.
- Lineage columns start with an underscore (`_batch_id`, `_ingest_ts`, `_source_file`) and are dropped before gold.
- Units are part of the name when ambiguous (`length_of_stay_days`).

## Why this matters six months from now

A new team member should be able to tell an item's environment, layer, and purpose from its name alone, and a permissions review should be able to reason about access by pattern (for example, "analysts read gold only") instead of item by item.
