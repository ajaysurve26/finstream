{{ set_azure_config() }}

WITH silver_data AS (
    SELECT *
    FROM {{ ref('stg_transactions') }}
    WHERE is_valid_record = true
),

country_fraud AS (
    SELECT
        country,
        is_high_risk_country,
        transaction_type,

        COUNT(*)                                            AS total_transactions,
        SUM(amount)                                         AS total_amount,
        SUM(CASE WHEN is_fraud = true THEN 1 ELSE 0 END)   AS fraud_count,
        SUM(CASE WHEN is_fraud = true THEN amount ELSE 0 END) AS fraud_amount,
        ROUND(
            SUM(CASE WHEN is_fraud = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2
        )                                                   AS fraud_rate_pct,
        COUNT(DISTINCT user_id)                             AS unique_users,
        AVG(amount)                                         AS avg_amount,

        current_timestamp()                                 AS dbt_processed_at

    FROM silver_data
    GROUP BY 1, 2, 3
)

SELECT * FROM country_fraud
ORDER BY fraud_rate_pct DESC