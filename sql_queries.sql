-- ============================================================
-- Project: Target Brazil E-commerce Data Analysis
-- Tool: SQL (BigQuery)
-- Dataset: 100K+ Orders (2016–2018)
-- ============================================================


-- ============================================================
-- SECTION 1: INITIAL EXPLORATION
-- ============================================================

-- 1.1 Data types of all columns in customers table

SELECT
    column_name,
    data_type
FROM target.INFORMATION_SCHEMA.COLUMNS
WHERE table_name = "customers";


-- 1.2 Get time range between which orders were placed

SELECT
    MIN(DATE(order_purchase_timestamp)) AS first_order_date,
    MAX(DATE(order_purchase_timestamp)) AS last_order_date
FROM target.orders;


-- 1.3 Count distinct cities and states of customers

SELECT
    COUNT(DISTINCT geolocation_city) AS no_of_cities,
    COUNT(DISTINCT geolocation_state) AS no_of_states
FROM target.geolocation;



-- ============================================================
-- SECTION 2: ORDER TRENDS ANALYSIS
-- ============================================================

-- 2.1 Year-wise number of orders

SELECT
    EXTRACT(YEAR FROM order_purchase_timestamp) AS year,
    COUNT(*) AS no_of_orders
FROM target.orders
GROUP BY year
ORDER BY year;


-- 2.2 Monthly seasonality of orders

WITH cte AS (
    SELECT
        EXTRACT(YEAR FROM order_purchase_timestamp) AS order_year,
        EXTRACT(MONTH FROM order_purchase_timestamp) AS order_month
    FROM target.orders
)

SELECT
    order_year,
    order_month,
    COUNT(*) AS no_of_orders
FROM cte
GROUP BY order_year, order_month
ORDER BY order_year, order_month;


-- 2.3 Orders by time of day

WITH cte AS (
    SELECT
        CASE
            WHEN EXTRACT(HOUR FROM order_purchase_timestamp) BETWEEN 0 AND 6 THEN 'Dawn'
            WHEN EXTRACT(HOUR FROM order_purchase_timestamp) BETWEEN 7 AND 12 THEN 'Morning'
            WHEN EXTRACT(HOUR FROM order_purchase_timestamp) BETWEEN 13 AND 18 THEN 'Afternoon'
            ELSE 'Night'
        END AS time_interval
    FROM target.orders
)

SELECT
    time_interval,
    COUNT(*) AS order_count
FROM cte
GROUP BY time_interval
ORDER BY order_count DESC;



-- ============================================================
-- SECTION 3: CUSTOMER DISTRIBUTION
-- ============================================================

-- 3.1 Month-on-month orders by state

SELECT
    c.customer_state,
    EXTRACT(MONTH FROM o.order_purchase_timestamp) AS order_month,
    COUNT(*) AS no_of_orders
FROM target.orders o
JOIN target.customers c
ON o.customer_id = c.customer_id
WHERE o.order_status NOT IN ('canceled', 'unavailable')
GROUP BY c.customer_state, order_month
ORDER BY c.customer_state, order_month;


-- 3.2 Customer distribution across states

SELECT
    customer_state,
    COUNT(customer_unique_id) AS no_of_unique_customers
FROM target.customers
GROUP BY customer_state
ORDER BY customer_state;



-- ============================================================
-- SECTION 4: REVENUE ANALYSIS
-- ============================================================

-- 4.1 Percentage increase in cost of orders from 2017 to 2018

WITH cte AS (
    SELECT
        o.order_id,
        p.payment_value,
        EXTRACT(YEAR FROM o.order_purchase_timestamp) AS order_year,
        EXTRACT(MONTH FROM o.order_purchase_timestamp) AS order_month
    FROM target.orders o
    JOIN target.payments p
    ON o.order_id = p.order_id
    WHERE EXTRACT(YEAR FROM o.order_purchase_timestamp) IN (2017, 2018)
    AND EXTRACT(MONTH FROM o.order_purchase_timestamp) BETWEEN 1 AND 8
),

cte1 AS (
    SELECT
        SUM(CASE WHEN order_year = 2017 THEN payment_value ELSE 0 END) AS revenue_2017,
        SUM(CASE WHEN order_year = 2018 THEN payment_value ELSE 0 END) AS revenue_2018
    FROM cte
)

SELECT
    revenue_2017,
    revenue_2018,
    ROUND(
        SAFE_DIVIDE(revenue_2018 - revenue_2017, revenue_2017) * 100,
        2
    ) AS percentage_increase
FROM cte1;



-- 4.2 Total and average order value by state

SELECT
    c.customer_state,
    ROUND(SUM(p.payment_value), 2) AS total_order_value,
    ROUND(AVG(p.payment_value), 2) AS average_order_value
FROM target.orders o
JOIN target.customers c
ON o.customer_id = c.customer_id
JOIN target.payments p
ON o.order_id = p.order_id
GROUP BY c.customer_state;



-- 4.3 Total and average freight value by state

SELECT
    c.customer_state,
    ROUND(SUM(ot.freight_value), 2) AS total_freight_value,
    ROUND(AVG(ot.freight_value), 2) AS average_freight_value
FROM target.orders o
JOIN target.customers c
ON o.customer_id = c.customer_id
JOIN target.order_items ot
ON o.order_id = ot.order_id
GROUP BY c.customer_state;



-- ============================================================
-- SECTION 5: DELIVERY TIME ANALYSIS
-- ============================================================

-- 5.1 Delivery time and difference between estimated and actual delivery

SELECT
    order_id,

    DATE_DIFF(
        DATE(order_delivered_customer_date),
        DATE(order_purchase_timestamp),
        DAY
    ) AS time_to_deliver,

    DATE_DIFF(
        DATE(order_estimated_delivery_date),
        DATE(order_delivered_customer_date),
        DAY
    ) AS diff_estimated_delivery

FROM target.orders;



-- 5.2 Top 5 states with highest and lowest average freight value

WITH high AS (
    SELECT
        c.customer_state,
        ROUND(AVG(ot.freight_value), 2) AS avg_freight,
        ROW_NUMBER() OVER (
            ORDER BY AVG(ot.freight_value) DESC
        ) AS rn
    FROM target.orders o
    JOIN target.customers c
    ON o.customer_id = c.customer_id
    JOIN target.order_items ot
    ON o.order_id = ot.order_id
    GROUP BY c.customer_state
),

low AS (
    SELECT
        c.customer_state,
        ROUND(AVG(ot.freight_value), 2) AS avg_freight,
        ROW_NUMBER() OVER (
            ORDER BY AVG(ot.freight_value) ASC
        ) AS rn
    FROM target.orders o
    JOIN target.customers c
    ON o.customer_id = c.customer_id
    JOIN target.order_items ot
    ON o.order_id = ot.order_id
    GROUP BY c.customer_state
)

SELECT
    h.customer_state AS highest_state,
    h.avg_freight AS highest_avg_freight,
    l.customer_state AS lowest_state,
    l.avg_freight AS lowest_avg_freight
FROM high h
JOIN low l
ON h.rn = l.rn
WHERE h.rn <= 5;



-- ============================================================
-- SECTION 6: PAYMENT ANALYSIS
-- ============================================================

-- 6.1 Month-on-month orders by payment type

SELECT
    FORMAT_TIMESTAMP("%Y-%m", o.order_purchase_timestamp) AS month,
    p.payment_type,
    COUNT(DISTINCT o.order_id) AS order_count
FROM target.orders o
JOIN target.payments p
ON o.order_id = p.order_id
GROUP BY month, p.payment_type
ORDER BY month;



-- 6.2 Orders by number of payment installments

SELECT
    payment_installments,
    COUNT(DISTINCT order_id) AS order_count
FROM target.payments
WHERE payment_installments <> 0
GROUP BY payment_installments
ORDER BY payment_installments;