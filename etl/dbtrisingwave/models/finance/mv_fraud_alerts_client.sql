{{ config(materialized = 'materialized_view') }}

SELECT c.id as client_id,
       c.name as client_name,
       c.category as client_category,
       f.card_id as card_id,
       f.window_start as window_start,
       f.window_end as window_end,
       f.total_amount as total_amount

FROM {{ ref('mv_fraud_alerts') }} f

LEFT OUTER JOIN {{ ref('credit_card_ownership') }} co
    ON f.card_id = co.card_id

LEFT OUTER JOIN {{ ref('src_iceberg_raw_clients') }} c
    ON co.client_id = c.id
