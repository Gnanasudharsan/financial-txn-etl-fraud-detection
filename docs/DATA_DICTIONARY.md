# Data dictionary

Types are Spark SQL types. `PK` = business key. Metadata columns start with `_`.

## Raw landing (strings unless noted)

| Feed | Format | Path |
|---|---|---|
| Card-present transactions (POS, ATM) | CSV | `transactions/source=card_present/event_date=*/part-cNNN.csv` |
| Card-not-present transactions (online) | JSON lines (25% of files carry drift columns `wallet_type`, `promo_code`) | `transactions/source=card_not_present/event_date=*/part-cNNN.json` |
| Fraud reports / chargebacks | CSV, delayed 1–45 days | `labels/` |
| Internal ledger | CSV | `ledger/posted_date=*/` |
| Processor settlement | CSV | `settlement/settlement_date=*/` |
| Dimensions | CSV | `dims/{customers,cards,merchants}.csv` |

## bronze

### bronze.transactions (partitioned by `_landing_date`)
| Column | Type | Notes |
|---|---|---|
| transaction_id … auth_code | string | as received (lat/lon typed `double` to detect shifted CSV lines) |
| card_token | string | HMAC-SHA256 of the PAN (PAN never stored) |
| card_last4 | string | |
| _corrupt_record | string | raw line when unparseable (PAN-redacted) |
| _rescued_data | string | JSON of unknown keys (schema drift) |
| _source_format / _source_system | string | csv/json; card_present/card_not_present |
| _source_file, _ingested_at, _run_id, _batch_id, _landing_date | | lineage |

### bronze._ingest_log
`dataset, source_key, source_uri, size, batch_id, run_id, ingested_at`, one row per ingested file.

## silver

### silver.transactions (partitioned by `event_date`; CHECK constraints)
| Column | Type | Notes |
|---|---|---|
| transaction_id | string PK | NOT NULL, unique (GX) |
| event_ts | timestamp | normalized to UTC from ISO-Z / ISO-offset / naive / epoch-ms |
| event_date | date | `= CAST(event_ts AS DATE)` (constraint) |
| card_token, card_last4 | string | token is 64 hex chars (constraint) |
| customer_id, merchant_id | string | RI-checked against dimensions |
| merchant_name, mcc | string | conformed from `silver.merchants` |
| amount | decimal(18,2) | > 0, in `currency` |
| currency | string | ISO 4217, in accepted set (constraint) |
| amount_usd | decimal(18,2) | via `silver.ref_fx_rates` |
| channel | string | POS / ONLINE / ATM |
| entry_mode | string | CHIP / CONTACTLESS / SWIPE / ECOM |
| latitude, longitude | double | range-checked |
| city, country, device_id, ip_address | string | |
| auth_status, auth_code | string | APPROVED / DECLINED |
| source_system, _source_file, _batch_id, _run_id, _ingested_at, _processed_at | | lineage |

### silver.transactions_quarantine
Raw values (`event_ts_raw`, `amount_raw`, `currency_raw`, …), `reason_codes array<string>`,
`primary_reason`, `_landing_date`, `_run_id`, `_quarantined_at`.

Reason codes (ordered by severity): `CORRUPT_RECORD`, `NULL_TRANSACTION_ID`, `NULL_EVENT_TS`,
`TS_UNPARSEABLE`, `TS_FUTURE`, `NULL_AMOUNT`, `AMOUNT_MALFORMED`, `AMOUNT_NON_POSITIVE`,
`NULL_CURRENCY`, `CURRENCY_INVALID`, `NULL_CARD`, `NULL_MERCHANT`, `NULL_CUSTOMER`,
`CHANNEL_INVALID`, `AUTH_STATUS_INVALID`, `RI_CARD_UNKNOWN`, `RI_MERCHANT_UNKNOWN`,
`RI_CUSTOMER_UNKNOWN`, `CARD_CUSTOMER_MISMATCH`, `DUPLICATE_EXACT`, `DUPLICATE_NEAR`.

### silver.customers
`customer_id` PK; `first_name_enc`, `last_name_enc`, `email_enc`, `phone_enc`,
`date_of_birth_enc` (AES-256-GCM, `v1:` prefix); analyst-safe `email_masked`, `phone_masked`,
`birth_year`; `home_city`, `home_country`, `home_utc_offset`, `home_lat`, `home_lon`,
`signup_date`, `segment`.

### silver.cards / silver.merchants / silver.ledger / silver.settlement / silver.fraud_labels
Typed versions of the raw dimensions and facts (money as `decimal(18,2)`, dates as `date`).
`cards` holds `card_token` + `card_last4` only.

### silver._dq_results
One row per GX expectation per run: `expectation, column, success, element_count,
unexpected_count, unexpected_percent, observed_value, validated_at`.

## gold

### gold.features_transactions (partitioned by `event_date`, Z-ORDER `card_token`)
Keys (`transaction_id`, `event_ts`, `card_token`, `customer_id`, `merchant_id`, `mcc`,
`channel`, `auth_status`), label (`is_fraud`, `fraud_type`), `split` (train/valid/test) and 28
features:

| Feature | Definition (strictly prior to the transaction) |
|---|---|
| amount_usd, log_amount | amount and log1p(amount) |
| card_txn_cnt_{1h,24h,7d}, card_amt_sum_{1h,24h,7d} | per-card count / USD sum in the trailing window |
| card_declines_24h | declined attempts on the card in the last 24 h |
| card_distinct_merchants_1h | distinct merchants on the card in the last hour |
| secs_since_last_txn | seconds since the card's previous transaction |
| cust_txn_count_prior | customer's prior transaction count |
| amount_zscore | (amount − trailing mean) / trailing std (≥ 5 prior txns) |
| amount_to_mean_ratio | amount / trailing mean |
| new_merchant_flag, new_device_flag | first time this customer used the merchant / device |
| geo_distance_km, implied_speed_kmh | haversine distance from previous txn and km/h needed |
| country_changed, is_foreign | country differs from previous txn / from home country |
| hour_local, day_of_week, is_night | in the customer's home time zone |
| channel_code, entry_mode_code | categorical codes |
| mcc_risk_score | smoothed MCC fraud rate fitted on the training window only |
| merchant_fraud_rate_lagged, merchant_txn_count_lagged | merchant fraud reports dated before the txn day / prior volume |

### gold.fct_transactions_scored (partitioned by `event_date`, Z-ORDER `merchant_id, card_token`)
Keys, `amount_usd`, `is_fraud`, `fraud_type`, `split`, `fraud_score` (0–1), `alert_flag`,
`model_version`, `threshold`, `scored_at`. `gold.fraud_alerts` is the `alert_flag = 1` subset.

### Marts
| Table | Grain | Main measures |
|---|---|---|
| daily_fraud_kpis | event_date × split | txn_count, amount_usd, fraud_count, fraud_amount_usd, alert_count, true/false positives, alert_precision, fraud_recall, fraud_rate_bps |
| merchant_risk | merchant | txn_count, fraud_count, fraud_rate, alert_count, avg/max score, risk_tier |
| customer_risk | customer (no PII) | txn_count, cards_used, fraud_count, alert_count, max_score, risk_tier |
| recon_results | ledger entry or settlement record | category, match_method, amounts, amount_diff, day_diff, is_break |
| recon_breaks | break records only | as above |
| reconciliation_summary | business_date × category | records, amount, abs_amount_diff, is_break |
| dq_health | landing_date | rows_total, rows_valid, rows_quarantined, pass_rate |
| dq_quarantine_by_reason | landing_date × reason × source | rows |

## audit.pipeline_runs
See [SECURITY.md](SECURITY.md#audit-trail).
