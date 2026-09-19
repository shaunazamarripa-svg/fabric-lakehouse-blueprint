# Decision log

Lightweight architecture decision records. Each entry states the context, the options considered, the decision, and the tradeoff accepted. Revisit an entry when its context changes.

| # | Decision | Status |
|---|---|---|
| 1 | One lakehouse per medallion layer | Accepted |
| 2 | Separate workspace per environment | Accepted |
| 3 | Bronze is append-only and all-string | Accepted |
| 4 | Quarantine bad rows instead of failing the load | Accepted |
| 5 | Gold is a star schema serving a Direct Lake semantic model | Accepted |
| 6 | Business logic lives in gold, not in reports | Accepted |
| 7 | PHI controls are layered, not a single switch | Accepted |

---

## 1. One lakehouse per medallion layer

**Context.** Bronze holds raw data that most people should never touch. Gold holds curated data most people should read.

**Options.** (a) One lakehouse with prefixed tables. (b) One lakehouse per layer.

**Decision.** One lakehouse per layer (`lh_bronze`, `lh_silver`, `lh_gold`) inside each environment's workspace.

**Tradeoff.** Cross-layer reads use OneLake paths (or shortcuts) instead of a simple table name, which is slightly more verbose. In return, layer-level permissions and lineage are obvious, and a mistake in bronze cannot be mistaken for a gold table.

## 2. Separate workspace per environment

**Context.** Changes must be testable before production, and production should not be edited by hand.

**Decision.** `ws_<domain>_dev`, `_test`, `_prod`, promoted through a deployment pipeline. Development happens in dev only.

**Tradeoff.** More workspaces to administer, and each needs capacity and access management. Verify which item types your tenant's deployment pipelines support before committing to this flow, and plan for the items that need manual or scripted promotion.

## 3. Bronze is append-only and all-string

**Context.** Source systems change their formats without warning, and the ability to replay history is valuable.

**Decision.** Read every column as string, add lineage columns, append each run with a batch ID. Silver decides which version of a record is current.

**Tradeoff.** Bronze grows over time and needs a retention policy. In return, nothing is lost, type surprises never break ingestion, and any silver table can be rebuilt from bronze.

## 4. Quarantine bad rows instead of failing the load

**Context.** One bad row should not block an entire morning refresh, but silently dropping rows destroys trust in the numbers.

**Decision.** Rows failing a contract rule are written to `<table>_quarantine` with the names of the rules they failed. Counts per table and run are written to `dq_results`.

**Tradeoff.** Consumers must monitor quarantine volume, so alerting on `dq_results` is part of the operating model. A hard failure would be safer for a small number of critical tables; the contract can be extended with a per-rule severity when needed.

## 5. Gold is a star schema serving a Direct Lake semantic model

**Context.** Report authors need a small, understandable model with consistent grain.

**Decision.** Conformed dimensions (`dim_*`) and additive facts (`fct_*`) in gold. Semantic models read gold via Direct Lake where the capacity supports it, with Import mode as the fallback.

**Tradeoff.** Direct Lake requires a Fabric-enabled capacity and has guardrails on model size and unsupported features. A star schema is more work up front than exposing silver tables, and prevents a long tail of report-specific joins.

## 6. Business logic lives in gold, not in reports

**Context.** When length of stay, AR aging buckets, or census are defined inside each report, they drift and finance and operations argue over which number is right.

**Decision.** Define each metric once in gold (or as a measure in the certified semantic model) and have reports consume it.

**Tradeoff.** Changes to a definition need a governed change process. That is the point: one definition, one owner, and a visible history.

## 7. PHI controls are layered, not a single switch

**Context.** This repo uses synthetic data, but the pattern is meant for regulated data.

**Decision.** In production, combine: workspace access by layer and role; sensitivity labels on items; row-level security in the semantic model by facility or region; masking or excluding direct identifiers before gold; no data values in notebook outputs or logs; and audit log review.

**Tradeoff.** More moving parts to configure and test. Every one of them fails independently, which is why they are layered.
