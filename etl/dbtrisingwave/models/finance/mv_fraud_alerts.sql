{{ config(materialized = 'materialized_view') }}

SELECT
    card_id,
    window_start,
    window_end,
    SUM(amount) AS total_amount
FROM
    TUMBLE(
        {{ ref('src_kafka_credit_card_transactions') }},  -- the source table
        ts,                                     -- the event timestamp column
        INTERVAL '1 minute'                     -- window size
    )
GROUP BY
    card_id, window_start, window_end
HAVING
    SUM(amount) > 5000  -- only alert if total spend exceeds $5000
