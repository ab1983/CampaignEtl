SELECT
    campaign_id,
    CAST(100.0 / COUNT(order_id)
        OVER (PARTITION BY order_id)
        AS NUMERIC(5, 2))
    attribution_perc,
    order_value / COUNT(order_id) OVER (PARTITION BY order_id) attribution_value
from (
    SELECT * FROM (
    SELECT
        order_id,
        guest_ref,
        order_value
    FROM orders WHERE order_date > (DATE('2021-04-24 00:00') - INTERVAL '30 DAY')
    ) orders
    INNER JOIN (
        SELECT 
            guest_ref,
            campaign_id
        FROM events WHERE event_type = 'CLICKED' AND event_date > (DATE('2021-04-24 00:00') - INTERVAL '30 DAY')
        ) events
    ON orders.guest_ref = events.guest_ref ) attributable_orders
;