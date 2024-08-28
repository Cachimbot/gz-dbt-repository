WITH calc AS (
    SELECT
        date_date,
        COUNT(orders_id) AS nb_transactions,
        ROUND(SUM(revenue)) AS revenue,
        ROUND((SUM(revenue) / COUNT(orders_id)),1) AS average_basket,
        ROUND(SUM(margin)) AS margin,
        ROUND(SUM(operational_margin)) AS operational_margin
    FROM
        {{ref("int_orders_operational")}}
    GROUP BY date_date
    ORDER BY date_date DESC
)

SELECT * FROM calc