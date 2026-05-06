-- Silver layer: Parse, clean and normalize raw Bronze JSON
-- This model reads raw JSON from Bronze and produces a clean structured table
{{ set_azure_config() }}

WITH bronze_raw AS (
    SELECT
        raw_payload,
        source_partition,
        source_offset,
        sequence_number,
        enqueued_time,
        ingestion_timestamp
    FROM delta.`abfss://bronze@finstreamdatalake.dfs.core.windows.net/transactions`
),

parsed AS (
    SELECT
        -- Parse JSON fields
        get_json_object(raw_payload, '$.transaction_id')        AS transaction_id,
        get_json_object(raw_payload, '$.user_id')               AS user_id,
        get_json_object(raw_payload, '$.merchant_name')         AS merchant_name,
        get_json_object(raw_payload, '$.merchant_category')     AS merchant_category,
        CAST(get_json_object(raw_payload, '$.amount') AS DOUBLE) AS amount,
        get_json_object(raw_payload, '$.currency')              AS currency,
        get_json_object(raw_payload, '$.country')               AS country,
        get_json_object(raw_payload, '$.transaction_type')      AS transaction_type,
        get_json_object(raw_payload, '$.card_last4')            AS card_last4,
        CAST(get_json_object(raw_payload, '$.timestamp') AS TIMESTAMP) AS transaction_timestamp,
        CAST(get_json_object(raw_payload, '$.hour_of_day') AS INT) AS hour_of_day,
        CAST(get_json_object(raw_payload, '$.is_fraud') AS BOOLEAN) AS is_fraud,
        CAST(get_json_object(raw_payload, '$.transaction_count_per_user') AS INT) AS transaction_count_per_user,

        -- Keep metadata columns for lineage
        source_partition,
        source_offset,
        ingestion_timestamp,
        enqueued_time

    FROM bronze_raw
),

cleaned AS (
    SELECT *,
        -- Add derived columns
        CASE
            WHEN amount > 8000 THEN 'high'
            WHEN amount > 1000 THEN 'medium'
            ELSE 'low'
        END AS amount_category,

        CASE
            WHEN country IN ('NG', 'RU', 'KP', 'IR', 'VE') THEN true
            ELSE false
        END AS is_high_risk_country,

        -- Data quality flag
        CASE
            WHEN transaction_id IS NULL THEN false
            WHEN amount IS NULL THEN false
            WHEN amount <= 0 THEN false
            WHEN user_id IS NULL THEN false
            ELSE true
        END AS is_valid_record,

        -- Add processing timestamp
        current_timestamp() AS dbt_processed_at

    FROM parsed
    WHERE transaction_id IS NOT NULL  -- Remove completely empty records
),

deduplicated AS (
    -- Remove duplicates keeping the latest record
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY transaction_id
            ORDER BY ingestion_timestamp DESC
        ) AS row_num
    FROM cleaned
)

SELECT * EXCEPT (row_num)
FROM deduplicated
WHERE row_num = 1  -- Keep only one record per transaction_id