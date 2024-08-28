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
        ROUND(revenue - (quantity * purchase_price),2) AS margin
    FROM
        source
)
SELECT * FROM calculations