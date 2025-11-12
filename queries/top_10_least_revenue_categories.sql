-- TODO: 
-- This query will return a table with the top 10 least revenue categories 
-- in English, the number of orders and their total revenue. 
-- It will have different columns: 
--      Category, that will contain the top 10 least revenue categories; 
--      Num_order, with the total amount of orders of each category; 
--      Revenue, with the total revenue of each category.

-- HINT: 
-- All orders should have a delivered status and the Category and actual delivery date should be not null.
-- For simplicity, if there are orders with multiple product categories, consider the full order's payment_value in the summation of revenue of each category
SELECT
    pcn.product_category_name_english AS Category,
    COUNT(DISTINCT o.order_id) AS Num_order,
    ROUND(SUM(op.payment_value), 2) AS Revenue
FROM olist_orders o
JOIN olist_order_items oi USING(order_id)
JOIN olist_products pr USING(product_id)
JOIN product_category_name_translation pcn USING(product_category_name)
JOIN olist_order_payments op USING(order_id)
WHERE 
    o.order_status = 'delivered'
    AND o.order_delivered_customer_date IS NOT NULL
    AND pcn.product_category_name_english IS NOT NULL
GROUP BY pcn.product_category_name_english
ORDER BY Revenue ASC
LIMIT 10;