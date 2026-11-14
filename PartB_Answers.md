# Part B — FreshCart ELT on Snowflake
### Assignment: From Git to Gold | Roll No: 23BCS246

---

## B1. Baseline Run (5 marks)

| Check | Expected | Actual |
|-------|----------|--------|
| Raw order rows in bronze after one run | 8 (6 web + 2 mobile) | ✅ 8 |
| Rows that pass cleaning (silver.orders_clean) | 6 | ✅ 6 |
| Rows in quarantine | 2 (ORD-1004, ORD-1006) | ✅ 2 |
| Orders after deduplication | 4 | ✅ 4 |
| Final gold line | 4 orders, revenue 7,785.00, 126 loyalty points | ✅ Confirmed |
| Share of rows rejected | 25% | ✅ 25% |

*(Screenshots attached in Appendix — B1_bronze, B1_silver, B1_quarantine, B1_dedup, B1_gold)*

---

## B2. Trace One Record by Hand (6 marks)

### ORD-1003 — Full Trace

#### Step 1 — bronze.orders_raw (two raw records)

ORD-1003 arrives **twice** — once from the web channel and once from the mobile app:

| Field | Web record (source = `web`) | Mobile record (source = `mobile`) |
|-------|----------------------------|-----------------------------------|
| order_id | `order_id: "ORD-1003"` | `orderId: "ORD-1003"` |
| email | `cust_email: "meera@outlook.com"` | `customer.email: "Meera@Outlook.com"` |
| amount | `amount: null` ← missing | `orderTotal: 2100` ← filled in |
| updated_at | `2026-09-02 09:10:00` | later timestamp (mobile re-send) |
| currency | `INR` | `INR` |

**Why the field names differ:** The web API uses flat, snake_case fields (`order_id`, `cust_email`, `amount`) while the mobile API uses a different schema with nested JSON and camelCase keys (`orderId`, `customer.email`, `orderTotal`). Both are stored as raw JSON in `bronze.orders_raw` exactly as received — this is the ELT principle: load first, clean later.

---

#### Step 2 — silver.orders_checked (SQL normalization)

`SQL_10_ORDERS_CLEAN` uses `COALESCE` to unify both schemas into identical column names:

```sql
COALESCE(raw:order_id::STRING,   raw:orderId::STRING)        AS order_id
COALESCE(raw:cust_email::STRING, raw:customer.email::STRING) AS email
COALESCE(raw:amount::NUMBER,     raw:orderTotal::NUMBER)      AS amount
TRIM(LOWER(email))                                            AS email  -- normalizes case & spaces
```

- The web record produces: `order_id=ORD-1003, email=meera@outlook.com, amount=NULL, amount_missing=TRUE`
- The mobile record produces: `order_id=ORD-1003, email=meera@outlook.com, amount=2100.00, amount_missing=FALSE`

Both pass the rejection filters (neither has a negative amount, bad email, or 5-digit pincode), so **both rows appear in `silver.orders_clean`**.

---

#### Step 3 — silver.orders_dedup (which copy survives?)

`SQL_20_ORDERS_DEDUP` deduplicates using:

```sql
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY order_id
    ORDER BY updated_at DESC, ingest_seq DESC
) = 1
```

- `PARTITION BY order_id` groups both ORD-1003 rows together.
- `ORDER BY updated_at DESC` ranks the **later** `updated_at` as row 1.
- The **mobile copy** has the later `updated_at` timestamp, so it gets `ROW_NUMBER() = 1`.
- `QUALIFY ... = 1` keeps only that row — the web copy is discarded.

**Result:** Only the mobile record survives — with `amount = 2100.00`.

---

#### Step 4 — gold.fct_orders (calculations)

Meera's customer tier on `order_date = 2026-09-02` is **Platinum** (from `dim.customers_scd2`).

From `ref.tier_multiplier`: Platinum → `multiplier = 3`

```
fx_rate      = 1.0000           (INR→INR, from ref.fx_daily)
amount_inr   = 2100.00 × 1.0   = 2100.00
loyalty_pts  = FLOOR(2100/100) × 3
             = 21 × 3
             = 63
is_high_value = (2100.00 ≥ 2000) = TRUE
```

✅ **Gold row for ORD-1003:** amount_inr = 2100.00, loyalty_points = 63

---

### ORD-1005 — Why Gold multiplier, not Silver (brief)

ORD-1005 is Priya's order on **2026-09-03** with amount = ₹899.

ORD-1001 is also Priya's order but on **2026-09-01** with amount = ₹1250.

**Priya earns Gold points (multiplier = 2) for ORD-1005 but Silver (multiplier = 1) for ORD-1001** because of a tier upgrade recorded in `dim.customers_scd2`.

The responsible table is **`dim.customers_scd2`** with the join condition:

```sql
LEFT JOIN dim.customers_scd2 c
       ON  c.email = o.email
       AND o.order_date BETWEEN c.valid_from AND c.valid_to
```

This is a **point-in-time join** — it picks Priya's tier that was **active on the specific order date**, not today's tier. On 2026-09-01, Priya's `valid_from–valid_to` range maps to **Silver** (multiplier = 1). By 2026-09-03, a new SCD2 row exists for Priya with tier = **Gold** (multiplier = 2), and that range covers her ORD-1005 order date.

**Loyalty point calculations:**
```
ORD-1001: FLOOR(1250/100) × 1 = 12 × 1 = 12 points  (Silver)
ORD-1005: FLOOR(899/100)  × 2 =  8 × 2 = 16 points  (Gold)
```

---

## B3. Add a Validation Rule (6 marks)

**SQL added inside `ARRAY_CONSTRUCT_COMPACT(...)` in SQL_10_ORDERS_CLEAN:**
```sql
IFF(amount <= 0, 'non_positive_amount', NULL),
IFF(currency NOT IN ('INR', 'USD'), 'unsupported_currency', NULL)   -- NEW LINE
```

**ORD-1006 reasons array BEFORE change:**
```
["bad_email", "bad_pincode"]
```
*(Screenshot: quarantine table from B1 run — Appendix B3-before)*

**ORD-1006 reasons array AFTER change:**
```
["bad_email", "bad_pincode", "unsupported_currency"]
```
*(Screenshot: silver.orders_checked query after re-run — Appendix B3-after)*

**Does the gold total change? Why or why not?**

No, the gold total does not change. ORD-1006 was already being sent to quarantine before the new rule (due to `bad_email` and `bad_pincode`). Adding `unsupported_currency` as an extra reason does not change the fact that it has `ARRAY_SIZE(reasons) > 0`, so it continues to be excluded from `silver.orders_clean` and never reaches `gold.fct_orders`. The gold table still shows 4 orders with revenue ₹7,785.00 and 126 loyalty points.

**Does a second quarantine row appear for ORD-1006?**

No. The quarantine `INSERT` statement contains a `NOT EXISTS` guard:

```sql
AND NOT EXISTS (
    SELECT 1 FROM quarantine.orders q
    WHERE q.source = c.source
      AND EQUAL_NULL(q.order_id, c.order_id)
      AND EQUAL_NULL(q.row_data:UPDATED_AT::TIMESTAMP_NTZ, c.updated_at)
)
```

This checks whether a row with the **same source, order_id, and updated_at** already exists in quarantine. Since ORD-1006 (source=web, updated_at=2026-09-04 08:30:00) was already quarantined in the first run, the `NOT EXISTS` condition evaluates to FALSE and the INSERT is skipped entirely. The quarantine table therefore still shows only the original row for ORD-1006, with the old reasons — but `silver.orders_checked` correctly reflects all three reasons in subsequent runs.

---

## B4. Change History and Idempotency (7 marks)

**New event added to `MOCK_CRM_TIER_EVENTS`:**
```python
{"cust_id": "CUST-02", "email": "arjun.k@yahoo.in", "tier": "Gold", "effective_from": "2026-08-15"}
```

### Prediction vs Actual Table:

| Quantity | Your Prediction | Actual after run | Match? |
|---|---|---|---|
| Rows for CUST-02 in `dim.customers_scd2` | 2 | 2 | ✅ |
| Tier used for ORD-1002 | Gold | Gold | ✅ |
| Loyalty points for ORD-1002 | 70 | 70 | ✅ |
| Total loyalty points in gold | 161 | 161 | ✅ |
| Number of gold rows whose `published_at` changed | 1 | 1 (ORD-1002) | ✅ |

*(Screenshot: dim.customers_scd2 and gold.fct_orders outputs — Appendix B4)*

### Explanation of Idempotency, `row_hash`, and Bronze Growth (4–6 sentences):

1. The `gold.fct_orders` table is kept idempotent through an `UPSERT` using Snowflake's `MERGE INTO` statement combined with a computed `row_hash` (generated via `HASH(...)` over all business columns).
2. On every pipeline execution, if an incoming record's `order_id` already exists in `gold.fct_orders` and its computed `row_hash` equals the stored `t.row_hash`, the `WHEN MATCHED AND t.row_hash <> s.row_hash` condition evaluates to false, so Snowflake does not update the record or alter its `published_at` timestamp.
3. Only records whose underlying values actually changed (such as ORD-1002 switching to the Gold tier and 70 points) trigger an `UPDATE SET` and update `published_at = CURRENT_TIMESTAMP()`.
4. In contrast, `bronze.orders_raw` represents an append-only raw landing layer: each invocation loads newly extracted file batches via `COPY INTO` without deduplication or state checking, preserving the full immutable audit trail of every arrival.
5. Consequently, bronze intentionally grows by 8 rows on every pipeline run, while gold maintains strict idempotency and business-level accuracy.

---

## B5. Add One Data-Quality Check (6 marks)

### Chosen Check:
**`orders in silver.orders_dedup whose order_date is in the future`**

### SQL Added to `quality_checks()`:
```python
"orders with future order date":
    "SELECT COUNT_IF(order_date > CURRENT_DATE()) FROM silver.orders_dedup",
```

### Purposeful Failure Demonstration (`PROBLEM`):
To test that this gate catches anomalies, a test order with a future timestamp was temporarily injected into `MOCK_WEB_ORDERS`:
```python
{"order_id": "ORD-9999", "cust_email": "test@freshcart.com", "cust_name": "Future Test", 
 "order_date": "2030-01-01", "amount": "500", "currency": "INR", "pincode": "600041", 
 "updated_at": "2030-01-01 10:00:00"}
```
Upon executing `run_pipeline()`, the check detected the anomaly and flagged it:
```text
4. CHECK
   rows gained or lost while enriching    0   OK
   duplicate orders in gold               0   OK
   orders with no customer match          1   PROBLEM
   orders with an amount but no FX rate   1   PROBLEM
   orders with no city                    0   OK
   orders with future order date          1   PROBLEM
   share of rows rejected                 25%
```
*(Screenshot: `b5_problem.png` showing the failed check — Appendix B5)*

After capturing the problem output, the mock record was removed and the pipeline returned to `0   OK`.

---

### Question: For FreshCart, why is ELT a better choice than ETL, and in which one situation from the lecture would ETL still win?

**Why ELT is better for FreshCart (3–4 sentences):**  
For FreshCart, ELT is vastly superior because incoming order streams arrive from multiple heterogeneous sources (web and mobile APIs) with varying, nested JSON schemas and frequent data quality discrepancies. By loading raw JSON directly into Snowflake's `bronze.orders_raw` without upfront alteration, FreshCart preserves a complete, immutable audit trail of the original raw events. All cleaning, deduplication, schema unification, and business enrichment logic are executed in SQL directly on cloud data warehouse compute, allowing developers to retrospectively modify validation rules (like the B3 currency check) or re-compute historical loyalty points (like the B4 SCD2 tier update) without needing to re-extract data from upstream APIs.

**When ETL would still win:**  
ETL still wins over ELT in situations involving strict privacy, legal, or regulatory compliance (such as GDPR, HIPAA, or PCI-DSS compliance). In these scenarios, highly sensitive personal or financial information—such as raw plaintext credit card numbers, passwords, or patient medical identifiers—must be masked, tokenized, or sanitized *in-flight* before the data is allowed to land on cloud storage or enter data warehouse disks.

---

