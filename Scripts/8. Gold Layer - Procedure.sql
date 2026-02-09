CREATE OR REPLACE PROCEDURE gold.load_gold()
LANGUAGE plpgsql
AS
$$
DECLARE
start_time TIMESTAMP;
end_time TIMESTAMP;
duration INTERVAL;
t_start_time TIMESTAMP;
t_end_time TIMESTAMP;
t_duration INTERVAL;
BEGIN
start_time := clock_timestamp();
RAISE NOTICE '============================================';
RAISE NOTICE 'LOADING GOLD LAYER';
RAISE NOTICE '============================================';


------------------------------------------------------
-- DIMENTION TABLE : dim_customers
------------------------------------------------------
t_start_time := clock_timestamp();
RAISE NOTICE '--------------------------------------------';
RAISE NOTICE 'LOADING TABLE : gold.dim_customers ';
RAISE NOTICE '--------------------------------------------';

RAISE NOTICE 'TRUNCATING TABLE : gold.dim_customers';
TRUNCATE TABLE gold.dim_customers;

RAISE NOTICE 'INSERTING DATA INTO : gold.dim_customers';
INSERT INTO gold.dim_customers
SELECT
customer_id,
name,
email,
phone,
city,
signup_date
FROM silver.customers;

t_end_time := clock_timestamp();
t_duration := t_end_time - t_start_time;
RAISE NOTICE 'LOADING TIME FOR TABLE gold.dim_customers: %',t_duration;



------------------------------------------------------
-- DIMENTION TABLE : dim_products
------------------------------------------------------
t_start_time := clock_timestamp();
RAISE NOTICE '--------------------------------------------';
RAISE NOTICE 'LOADING TABLE : gold.dim_products ';
RAISE NOTICE '--------------------------------------------';

RAISE NOTICE 'TRUNCATING TABLE : gold.dim_products';
TRUNCATE TABLE gold.dim_products;

RAISE NOTICE 'INSERTING DATA INTO : gold.dim_products';
INSERT INTO gold.dim_products
SELECT
DISTINCT
product_id
FROM silver.order_items;

t_end_time := clock_timestamp();
t_duration := t_end_time - t_start_time;
RAISE NOTICE 'LOADING TIME FOR TABLE gold.dim_products: %',t_duration;



------------------------------------------------------
-- DIMENTION TABLE : dim_dates
------------------------------------------------------
t_start_time := clock_timestamp();
RAISE NOTICE '--------------------------------------------';
RAISE NOTICE 'LOADING TABLE : gold.dim_dates ';
RAISE NOTICE '--------------------------------------------';

RAISE NOTICE 'TRUNCATING TABLE : gold.dim_dates';
TRUNCATE TABLE gold.dim_dates;

RAISE NOTICE 'INSERTING DATA INTO : gold.dim_dates';
INSERT INTO gold.dim_dates
SELECT DISTINCT
  order_ts::DATE AS date,
  EXTRACT(YEAR FROM order_ts) AS year,
  EXTRACT(MONTH FROM order_ts) AS month,
  EXTRACT(DAY FROM order_ts) AS day,
  TO_CHAR(order_ts, 'Day') AS weekday
FROM silver.orders;

t_end_time := clock_timestamp();
t_duration := t_end_time - t_start_time;
RAISE NOTICE 'LOADING TIME FOR TABLE gold.dim_dates: %',t_duration;


------------------------------------------------------
-- FACT TABLE : fact_orders
------------------------------------------------------
t_start_time := clock_timestamp();
RAISE NOTICE '--------------------------------------------';
RAISE NOTICE 'LOADING TABLE : gold.fact_orders ';
RAISE NOTICE '--------------------------------------------';

RAISE NOTICE 'TRUNCATING TABLE : gold.fact_orders';
TRUNCATE TABLE gold.fact_orders;

RAISE NOTICE 'INSERTING DATA INTO : gold.fact_orders';
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
on o.order_id = s.order_id;

t_end_time := clock_timestamp();
t_duration := t_end_time - t_start_time;
RAISE NOTICE 'LOADING TIME FOR TABLE gold.fact_orders: %',t_duration;


------------------------------------------------------
-- FACT TABLE : fact_order_items
------------------------------------------------------
t_start_time := clock_timestamp();
RAISE NOTICE '--------------------------------------------';
RAISE NOTICE 'LOADING TABLE : gold.fact_order_items ';
RAISE NOTICE '--------------------------------------------';

RAISE NOTICE 'TRUNCATING TABLE : gold.fact_order_items';
TRUNCATE TABLE gold.fact_order_items;

RAISE NOTICE 'INSERTING DATA INTO : gold.fact_order_items';
INSERT INTO gold.fact_order_items
SELECT
order_id,
product_id,
quantity,
unit_price,
(quantity*unit_price) AS revenue
FROM silver.order_items;

t_end_time := clock_timestamp();
t_duration := t_end_time - t_start_time;
RAISE NOTICE 'LOADING TIME FOR TABLE gold.fact_order_items: %',t_duration;


------------------------------------------------------
-- FACT TABLE : fact_order_items
------------------------------------------------------
t_start_time := clock_timestamp();
RAISE NOTICE '--------------------------------------------';
RAISE NOTICE 'LOADING TABLE : gold.fact_payments ';
RAISE NOTICE '--------------------------------------------';

RAISE NOTICE 'TRUNCATING TABLE : gold.fact_payments';
TRUNCATE TABLE gold.fact_payments;

RAISE NOTICE 'INSERTING DATA INTO : gold.fact_payments';
INSERT INTO gold.fact_payments
SELECT
payment_id,
order_id,
payment_amount,
payment_status,
payment_ts AS payment_times
FROM silver.payments;

t_end_time := clock_timestamp();
t_duration := t_end_time - t_start_time;
RAISE NOTICE 'LOADING TIME FOR TABLE gold.fact_payments: %',t_duration;



end_time := clock_timestamp();
duration := end_time - start_time;
RAISE NOTICE '============================================';
RAISE NOTICE 'TOTAL LOADING TIME : %', duration;
RAISE NOTICE '============================================';
END
$$


--call gold.load_gold()