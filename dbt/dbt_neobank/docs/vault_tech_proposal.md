# 🧱 Vault Modeling Tech Proposal – Neobank

This document outlines the proposed architecture and logic for building a Data Vault model for the Neobank project using dbt. The approach follows best practices with clear separation of concerns:

- Staging Layer (stg)
- Hubs
- Links
- Satellites

---

## 1️⃣ Staging Models (`stg_*`)

These models transform raw input into structured, cleaned, and ready-to-model tables.

| Model             | Source Table         | Key Columns                     | Transformations                                |
|------------------|----------------------|----------------------------------|------------------------------------------------|
| `stg_users`      | `raw_users`          | `user_id`                        | Rename fields, cast timestamps                |
| `stg_transactions`| `raw_transactions`  | `transaction_id`, `user_id`     | Rename fields, timestamp conversion           |
| `stg_devices`    | `raw_devices`        | `user_id`, `device_type`        | Device normalization                          |
| `stg_notifications` | `raw_notifications` | `user_id`, `channel`, `created_at` | Field renaming, timestamp formatting       |

---

## 2️⃣ Hubs

| Hub Name           | Business Key(s)                       | Source            |
|--------------------|----------------------------------------|-------------------|
| `hub_user`         | `user_id`                              | `stg_users`       |
| `hub_transaction`  | `transaction_id`                       | `stg_transactions`|
| `hub_notification` | `user_id`, `channel`, `created_at`     | `stg_notifications`|

---

## 3️⃣ Links

| Link Name               | Foreign Keys                         | Source              |
|-------------------------|--------------------------------------|---------------------|
| `link_user_device`      | `user_id`, `device_type`             | `stg_devices`       |
| `link_user_plan`        | `user_id`, `plan`                    | `stg_users`         |
| `link_transaction_user` | `transaction_id`, `user_id`         | `stg_transactions`  |
| `link_user_notification`| `user_id`, `channel`, `created_at`  | `stg_notifications` |

---

## 4️⃣ Satellites

| Satellite                | Attached To        | Descriptive Fields                              |
|--------------------------|--------------------|-------------------------------------------------|
| `sat_user_attributes`    | `hub_user`         | `birth_year`, `country`, `crypto_unlocked`, `num_contacts`, etc. |
| `sat_transaction_details`| `hub_transaction`  | `transaction_type`, `currency`, `amount_usd`, etc. |
| `sat_notification_details`| `hub_notification`| `reason`, `status`                              |
| `sat_device_metadata`    | `link_user_device` | `device_type`                                   |

---

## 🧪 Validation Strategy

- ✅ `dbt compile` ran successfully (no syntax errors).
- 🛑 `dbt run` not executed – to avoid writing to BigQuery prematurely.
- ✅ Source columns verified via exported CSV.
- ✅ Manual review of staging SQLs confirmed correct field mappings.

---

## ✅ Status Summary

| Component         | Status      |
|------------------|-------------|
| Staging Models   | ✅ Completed|
| Hubs             | ✅ Completed|
| Links            | ✅ Completed|
| Satellites       | ✅ Completed|
| Compilation      | ✅ Passed   |
| Run to BQ        | ❌ Skipped  |
