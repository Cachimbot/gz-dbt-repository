WITH source AS (
    SELECT *
    FROM
        {{ ref("int_orders_margin") }} AS sales
    JOIN
        {{ ref("stg_raw__ship") }} AS product
    USING(orders_id)
),
calculations AS (
    SELECT
        *,
        margin + shipping_fee - log_cost - ship_cost AS operational_margin,
    FROM
        source
)
SELECT
    orders_id,
    date_date,
    ROUND(SUM(revenue),2) AS revenue,
    ROUND(SUM(quantity),2) AS quantity,
    ROUND(SUM(purchase_cost),2) AS purchase_cost,
    ROUND(SUM(margin),2) AS margin,
    ROUND(SUM(operational_margin),2) AS operational_margin
FROM
    calculations
GROUP BY
    orders_id, date_date
ORDER BY
    orders_id DESC, date_date DESC