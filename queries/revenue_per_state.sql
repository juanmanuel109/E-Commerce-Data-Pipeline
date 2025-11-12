-- TODO: 
-- This query will return a table with two columns: customer_state and Revenue. 
-- The first one will have the letters that identify the top 10 states 
-- with most revenue and the second one the total revenue of each.

-- HINT: 
-- All orders should have a delivered status and the actual delivery date should be not null. 
SELECT
    c.customer_state AS customer_state,
    ROUND(SUM(p.payment_value), 2) AS Revenue

    
FROM olist_orders AS o
JOIN olist_customers AS c
    USING(customer_id)
JOIN (
    SELECT order_id, SUM(payment_value) AS payment_value
    FROM olist_order_payments
    GROUP BY order_id
) AS p
    USING(order_id)
WHERE o.order_status = 'delivered'
  AND o.order_delivered_customer_date IS NOT NULL
GROUP BY c.customer_state
ORDER BY Revenue DESC
LIMIT 10;