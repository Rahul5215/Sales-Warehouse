DROP TABLE IF EXIST silver.orders;
CREATE TABLE silver.orders(
order_id VARCHAR(50),
order_ts TIMESTAMP,
customer_id VARCHAR(50),
order_status VARCHAR(50),
order_amount DECIMAL(10,2),
currency VARCHAR(50),

source_system VARCHAR(50),
updated_at TIMESTAMP
);

TRUNCATE TABLE silver.orders
INSERT INTO silver.orders(order_id, order_ts, customer_id, order_status, order_amount, currency, source_system, updated_at)
SELECT
order_id,
NULLIF(order_ts, '')::TIMESTAMP AS order_ts,
COALESCE(customer_id, 'C0000') AS customer_id, 
UPPER(order_status) AS order_status,
CAST(order_amount AS DECIMAL(10,2)),
currency,
source_system,
CAST(updated_at AS TIMESTAMP)
FROM
(
select
*,
row_number() over(partition by order_id order by updated_at desc) as ranking
from bronze.orders_raw
)
where ranking = 1



DROP TABLE IF EXIST silver.order_items;
CREATE TABLE silver.order_items(
order_id VARCHAR(50),
product_id VARCHAR(50),
quantity INT,
unit_price DECIMAL(10,2)
);

TRUNCATE TABLE silver.order_items
INSERT INTO silver.order_items
select
order_id,
product_id,
case when trim(quantity) = 'two' then '2'
     when trim(quantity) = 'one' then '1'
	 else trim(quantity)::INTEGER
end as quantity,
CAST(unit_price AS DECIMAL(10,2))
from bronze.order_items_raw


DROP TABLE IF EXIST silver.customers;
CREATE TABLE silver.customers(
customer_id VARCHAR(50) PRIMARY KEY,
name VARCHAR(50) NOT NULL,
email VARCHAR(50) NOT NULL,
phone VARCHAR(20),
city VARCHAR(50),
signup_date DATE,
updated_at TIMESTAMP
);

TRUNCATE TABLE silver.customers;
INSERT INTO silver.customers
select
customer_id,
COALESCE(name,'Unknown Customer') AS name,
email,
CASE 
   WHEN phone IS NULL THEN 'UNKNOWN NUMBER'
   WHEN phone ~ '^[0-9]{10}$' THEN phone
   ELSE 'INVALID NUMBER'
END AS phone,
INITCAP(city) AS city,
signup_date::date AS signup_date,
updated_at::TIMESTAMP AS updated_at
FROM
(
SELECT
*,
row_number() over(partition by customer_id order by updated_at desc) as ranking
from bronze.customers_raw
)
WHERE ranking = 1



DROP TABLE IF EXIST silver.payments;
CREATE TABLE silver.payments(
payment_id VARCHAR(50),
order_id VARCHAR(50),
payment_mode VARCHAR(50),
payment_amount DECIMAL(10,2),
payment_status VARCHAR(50),
payment_ts DATE
);

TRUNCATE TABLE silver.payments;
INSERT INTO silver.payments
select
payment_id,
order_id,
payment_mode,
CAST(payment_amount AS DECIMAL(10,2)),
payment_status,
CAST(payment_ts AS DATE)
from bronze.payments_raw


DROP TABLE IF EXIST silver.shipments;
CREATE TABLE silver.shipments(
shipment_id VARCHAR(50),
order_id VARCHAR(50),
shipped_date DATE,
is_shipped BOOLEAN,
delivered_date DATE,
is_delivered BOOLEAN,
carrier VARCHAR(50)
);

TRUNCATE TABLE silver.shipments;
INSERT INTO silver.shipments
SELECT
CASE WHEN shipment_id LIKE 'SHP%'
     OR shipment_id LIKE 'SH%'
	 OR shipment_id LIKE 'SP%'
	 THEN 'SHP-' || regexp_replace(shipment_id,'[^0-9]','','g')
     ELSE shipment_id
END AS shipment_id,
order_id,
CAST(shipped_date AS DATE),
CASE WHEN shipped_date IS NOT NULL THEN TRUE
     ELSE FALSE
END AS is_shipped,
CAST(delivered_date AS DATE),
CASE WHEN delivered_date IS NOT NULL THEN TRUE
     ELSE FALSE
END AS is_delivered,
carrier
FROM bronze.shipments_raw