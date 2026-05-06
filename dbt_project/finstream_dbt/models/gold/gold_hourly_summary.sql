{{ set_azure_config() }}

WITH silver_data AS (
    SELECT *
    FROM {{ ref('stg_transactions') }}
    WHERE is_valid_record = true
),

hourly_agg AS (
    SELECT
        DATE_TRUNC('hour', transaction_timestamp)   AS transaction_hour,
        merchant_category,
        currency,
        country,

        -- Volume metrics
        COUNT(*)                                    AS total_transactions,
        COUNT(DISTINCT user_id)                     AS unique_users,

        -- Amount metrics
        SUM(amount)                                 AS total_amount,
        AVG(amount)                                 AS avg_amount,
        MIN(amount)                                 AS min_amount,
        MAX(amount)                                 AS max_amount,

        -- Fraud metrics
        SUM(CASE WHEN is_fraud = true THEN 1 ELSE 0 END)   AS fraud_count,
        SUM(CASE WHEN is_fraud = true THEN amount ELSE 0 END) AS fraud_amount,
        ROUND(
            SUM(CASE WHEN is_fraud = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2
        )                                                   AS fraud_rate_pct,

        -- Risk metrics
        SUM(CASE WHEN is_high_risk_country = true THEN 1 ELSE 0 END) AS high_risk_country_count,

        current_timestamp()                         AS dbt_processed_at

    FROM silver_data
    GROUP BY 1, 2, 3, 4
)

SELECT * FROM hourly_agg