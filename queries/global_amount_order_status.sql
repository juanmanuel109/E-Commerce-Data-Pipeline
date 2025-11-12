-- TODO: 
-- This query will return a table with two columns: order_status and Amount. 
-- The first one will have the different order status classes 
-- and the second one the total amount of each.

SELECT
    o.order_status AS order_status,
    COUNT(o.order_id) AS Amount
FROM olist_orders AS o
GROUP BY o.order_status
ORDER BY o.order_status ASC;