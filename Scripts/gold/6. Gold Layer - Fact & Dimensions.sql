/*
🥇 Gold Layer – Analytical Star Schema

The Gold layer represents the business-ready analytical model built on top of the cleaned Silver layer. It follows a star schema design by separating transactional metrics (fact tables) from descriptive attributes (dimension tables).

Fact Tables:

fact_orders – Order-level transactional data enriched with shipment attributes

fact_order_items – Line-item level sales metrics including derived revenue

fact_payments – Payment transaction details

Dimension Tables:

dim_customers – Customer master data for reporting and segmentation

dim_products – Product reference dimension derived from order items

This layer is designed to support analytical workloads such as:

Revenue analysis

Customer behavior tracking

Shipment performance monitoring

Payment success analysis

Product performance reporting

The Gold layer enables efficient aggregations and BI tool integration by providing structured, analytics-ready datasets.
*/

SELECT * FROM silver.orders

SELECT * FROM silver.shipments

SELECT * FROM silver.order_items

SELECT * FROM silver.payments

SELECT * FROM silver.customers

SELECT * FROM gold.fact_orders

------------------------------------------------------
-- FACT TABLES:
------------------------------------------------------
DROP TABLE gold.fact_orders;
CREATE TABLE gold.fact_orders AS
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
s.is_delivered
FROM silver.orders o
LEFT JOIN silver.shipments s
on o.order_id = s.order_id


CREATE TABLE gold.fact_order_items AS
SELECT
order_id,
product_id,
quantity,
unit_price,
(quantity*unit_price) AS revenue
FROM silver.order_items

CREATE TABLE gold.fact_payments AS
SELECT
payment_id,
order_id,
payment_amount,
payment_status,
payment_ts
FROM silver.payments


------------------------------------------------------
-- DIMENSTION TABLES:
------------------------------------------------------
CREATE TABLE gold.dim_customers as
SELECT
customer_id,
name,
email,
phone,
city,
signup_date
FROM silver.customers


CREATE TABLE gold.dim_products AS
SELECT
DISTINCT
product_id
FROM silver.order_items


