------------------------------------------------------
-- FACT TABLE : fact_orders
------------------------------------------------------
DROP TABLE IF EXISTS gold.fact_orders;
CREATE TABLE gold.fact_orders (
  order_id        VARCHAR(50) PRIMARY KEY,
  order_ts        TIMESTAMP,
  customer_id     VARCHAR(50),
  order_status    VARCHAR(30),
  order_amount    DECIMAL(12,2),
  currency        VARCHAR(10),
  source_system   VARCHAR(20),
  shipment_id     VARCHAR(50),
  carrier         VARCHAR(50),
  shipped_date    DATE,
  is_shipped      BOOLEAN,
  delivered_date  DATE,
  is_delivered    BOOLEAN,
  created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO gold.fact_orders
SELECT
o.order_id,
o.order_ts,
o.customer_id,
o.order_status,
o.order_amount,
o.currency,
o.source_system,
s.shipment_id,
s.carrier,
s.shipped_date,
s.is_shipped,
s.delivered_date,
s.is_delivered
FROM silver.orders o
LEFT JOIN silver.shipments s
on o.order_id = s.order_id


------------------------------------------------------
-- FACT TABLE : fact_order_items
------------------------------------------------------
DROP TABLE IF EXISTS gold.fact_order_items;
CREATE TABLE gold.fact_order_items(
order_id VARCHAR(50),
product_id VARCHAR(50),
quantity INT,
unit_price DECIMAL(10,2),
revenue DECIMAL(10,2)
);
INSERT INTO gold.fact_order_items
SELECT
order_id,
product_id,
quantity,
unit_price,
(quantity*unit_price) AS revenue
FROM silver.order_items


------------------------------------------------------
-- FACT TABLE : fact_payments
------------------------------------------------------
DROP TABLE IF EXISTS gold.fact_payments;
CREATE TABLE gold.fact_payments(
payment_id VARCHAR(50),
order_id VARCHAR(50),
payment_amount DECIMAL(10,2),
payment_status VARCHAR(50),
payment_times TIMESTAMP
);
INSERT INTO gold.fact_payments
SELECT
payment_id,
order_id,
payment_amount,
payment_status,
payment_ts AS payment_times
FROM silver.payments

------------------------------------------------------
-- DIMENTION TABLE : dim_customers
------------------------------------------------------
DROP TABLE IF EXISTS gold.dim_customers;
CREATE TABLE gold.dim_customers(
customer_id VARCHAR(50),
name VARCHAR(50),
email VARCHAR(50),
phone VARCHAR(50),
city VARCHAR(50),
signup_date DATE
);
INSERT INTO gold.dim_customers
SELECT
customer_id,
name,
email,
phone,
city,
signup_date
FROM silver.customers

------------------------------------------------------
-- DIMENTION TABLE : dim_products
------------------------------------------------------
DROP TABLE IF EXISTS gold.dim_products;
CREATE TABLE gold.dim_products(
product_id VARCHAR(50),
product_name VARCHAR(50),
category VARCHAR(50)
);
INSERT INTO gold.dim_products
SELECT
DISTINCT
product_id,
product_name,
category
FROM silver.order_items

------------------------------------------------------
-- DIMENTION TABLE : dim_dates
------------------------------------------------------
DROP TABLE IF EXISTS gold.dim_dates
CREATE TABLE gold.dim_dates
SELECT DISTINCT
  order_ts::DATE AS date,
  EXTRACT(YEAR FROM order_ts) AS year,
  EXTRACT(MONTH FROM order_ts) AS month,
  EXTRACT(DAY FROM order_ts) AS day,
  TO_CHAR(order_ts, 'Day') AS weekday
FROM silver.orders;
