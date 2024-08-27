WITH source AS (
    SELECT *
    FROM
        {{ ref("stg_raw__sales") }} AS sales
    LEFT JOIN
        {{ ref("stg_raw__product") }} AS product
    USING(products_id)
),
calculations AS (
    SELECT
        *,
        quantity * purchase_price AS purchase_cost,
        revenue - (quantity * purchase_price) AS margin
    FROM
        source
)
SELECT
    orders_id,
    date_date,
    ROUND(SUM(revenue),2) AS revenue,
    ROUND(SUM(quantity),2) AS quantity,
    ROUND(SUM(purchase_cost),2) AS purchase_cost,
    ROUND(SUM(margin),2) AS margin
FROM
    calculations
GROUP BY
    orders_id, date_date
ORDER BY
    2 DESC