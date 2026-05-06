{{ set_azure_config() }}

WITH silver_data AS (
    SELECT *
    FROM {{ ref('stg_transactions') }}
    WHERE is_valid_record = true
),

user_stats AS (
    SELECT
        user_id,

        -- Transaction behaviour
        COUNT(*)                                        AS total_transactions,
        SUM(amount)                                     AS total_spend,
        AVG(amount)                                     AS avg_transaction_amount,
        MAX(amount)                                     AS max_transaction_amount,
        COUNT(DISTINCT merchant_category)               AS unique_merchant_categories,
        COUNT(DISTINCT country)                         AS unique_countries,
        COUNT(DISTINCT currency)                        AS unique_currencies,

        -- Fraud signals
        SUM(CASE WHEN is_fraud = true THEN 1 ELSE 0 END)       AS fraud_transaction_count,
        SUM(CASE WHEN is_high_risk_country THEN 1 ELSE 0 END)  AS high_risk_country_txns,
        SUM(CASE WHEN hour_of_day BETWEEN 2 AND 5 THEN 1 ELSE 0 END) AS late_night_txns,

        -- Risk score (0-100)
        LEAST(100, (
            SUM(CASE WHEN is_fraud = true THEN 10 ELSE 0 END) +
            SUM(CASE WHEN is_high_risk_country THEN 5 ELSE 0 END) +
            SUM(CASE WHEN hour_of_day BETWEEN 2 AND 5 THEN 2 ELSE 0 END) +
            CASE WHEN MAX(amount) > 8000 THEN 15 ELSE 0 END
        ))                                              AS risk_score,

        -- Risk category
        CASE
            WHEN LEAST(100, (
                SUM(CASE WHEN is_fraud = true THEN 10 ELSE 0 END) +
                SUM(CASE WHEN is_high_risk_country THEN 5 ELSE 0 END) +
                SUM(CASE WHEN hour_of_day BETWEEN 2 AND 5 THEN 2 ELSE 0 END) +
                CASE WHEN MAX(amount) > 8000 THEN 15 ELSE 0 END
            )) >= 50 THEN 'high'
            WHEN LEAST(100, (
                SUM(CASE WHEN is_fraud = true THEN 10 ELSE 0 END) +
                SUM(CASE WHEN is_high_risk_country THEN 5 ELSE 0 END) +
                SUM(CASE WHEN hour_of_day BETWEEN 2 AND 5 THEN 2 ELSE 0 END) +
                CASE WHEN MAX(amount) > 8000 THEN 15 ELSE 0 END
            )) >= 20 THEN 'medium'
            ELSE 'low'
        END                                             AS risk_category,

        MIN(transaction_timestamp)                      AS first_transaction_at,
        MAX(transaction_timestamp)                      AS last_transaction_at,
        current_timestamp()                             AS dbt_processed_at

    FROM silver_data
    GROUP BY user_id
)

SELECT * FROM user_stats