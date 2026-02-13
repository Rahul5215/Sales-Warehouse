/*
Silver Layer – Transformation Procedure

This procedure implements the Bronze → Silver transformation layer of the warehouse. It performs structured data cleansing, deduplication, standardization, and schema enforcement to convert raw operational data into analytics-ready datasets.

Key transformations include:

Deduplication using ROW_NUMBER() to retain the most recent records

Data type enforcement (TIMESTAMP, DATE, DECIMAL, INTEGER)

Null handling and default value assignment

Domain standardization (UPPER, INITCAP)

Shipment ID normalization using regex

Derived boolean indicators for shipment status

Full-refresh load strategy using TRUNCATE + INSERT

Execution time tracking and structured error diagnostics

This layer ensures that downstream analytical models operate on consistent, validated, and structured data.
*/

CREATE OR REPLACE PROCEDURE silver.load_silver()
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
v_state     text;
v_message   text;
v_detail 	text;
v_hint 		text;
BEGIN
start_time := clock_timestamp();
RAISE NOTICE '============================================';
RAISE NOTICE 'LOADING SILVER LAYER';
RAISE NOTICE '============================================';

------------------------------------------------------
-- ORDER_ITEMS
------------------------------------------------------

RAISE NOTICE '--------------------------------------------';
RAISE NOTICE 'LOADING TABLE : silver.order_items';
RAISE NOTICE '--------------------------------------------';

t_start_time := clock_timestamp();
RAISE NOTICE 'TRUNCATING TABLE : silver.order_items';
TRUNCATE TABLE silver.order_items;

RAISE NOTICE 'INSERTING DATA INTO : silver.order_items';
INSERT INTO silver.order_items
select
order_id,
product_id,
case when trim(quantity) = 'two' then '2'
     when trim(quantity) = 'one' then '1'
	 else trim(quantity)::INTEGER
end as quantity,
CAST(unit_price AS DECIMAL(10,2))
from bronze.order_items;

t_end_time := clock_timestamp();
t_duration := t_end_time - t_start_time;
RAISE NOTICE 'LOADING TIME FOR TABLE silver.oreder_items: %',t_duration;

------------------------------------------------------
-- ORDERS
------------------------------------------------------
RAISE NOTICE '--------------------------------------------';
RAISE NOTICE 'LOADING TABLE : silver.orders';
RAISE NOTICE '--------------------------------------------';

t_start_time := clock_timestamp();
RAISE NOTICE 'TRUNCATING TABLE : silver.orders';
TRUNCATE TABLE silver.orders;

RAISE NOTICE 'INSERTING DATA INTO : silver.orders';
INSERT INTO silver.orders
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
where ranking = 1;

t_end_time := clock_timestamp();
t_duration := t_end_time - t_start_time;
RAISE NOTICE 'LOADING TIME FOR TABLE silver.orders: %',t_duration;

------------------------------------------------------
-- CUSTOMERS
------------------------------------------------------
RAISE NOTICE '--------------------------------------------';
RAISE NOTICE 'LOADING TABLE : silver.customers';
RAISE NOTICE '--------------------------------------------';

t_start_time := clock_timestamp();
RAISE NOTICE 'TRUNCATING TABLE : silver.customers';
TRUNCATE TABLE silver.customers;

RAISE NOTICE 'INSERTING DATA INTO : silver.customers';
INSERT INTO silver.customers
SELECT
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
ROW_NUMBER() OVER(PARTITION BY customer_id ORDER BY updated_at DESC) AS ranking
FROM bronze.customers_raw
)
WHERE ranking = 1;

t_end_time := clock_timestamp();
t_duration := t_end_time - t_start_time;
RAISE NOTICE 'LOADING TIME FOR TABLE silver.customers: %',t_duration;

------------------------------------------------------
-- PAYMENTS
------------------------------------------------------
RAISE NOTICE '--------------------------------------------';
RAISE NOTICE 'LOADING TABLE : silver.payments';
RAISE NOTICE '--------------------------------------------';

t_start_time := clock_timestamp();
RAISE NOTICE 'TRUNCATING TABLE : silver.payments';
TRUNCATE TABLE silver.payments;

RAISE NOTICE 'INSERTING DATA INTO : silver.payments';
INSERT INTO silver.payments
SELECT
payment_id,
order_id,
payment_mode,
CAST(payment_amount AS DECIMAL(10,2)),
payment_status,
CAST(payment_ts AS DATE)
FROM bronze.payments_raw;

t_end_time := clock_timestamp();
t_duration := t_end_time - t_start_time;
RAISE NOTICE 'LOADING TIME FOR TABLE silver.payments: %',t_duration;

------------------------------------------------------
-- SHIPMENTS
------------------------------------------------------
RAISE NOTICE '--------------------------------------------';
RAISE NOTICE 'LOADING TABLE : silver.shipments';
RAISE NOTICE '--------------------------------------------';

t_start_time := clock_timestamp();
RAISE NOTICE 'TRUNCATING TABLE : silver.shipments';
TRUNCATE TABLE silver.shipments;

RAISE NOTICE 'INSERTING DATA INTO : silver.shipments';
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
FROM bronze.shipments_raw;

t_end_time := clock_timestamp();
t_duration := t_end_time - t_start_time;
RAISE NOTICE 'LOADING TIME FOR TABLE silver.shipments: %',t_duration;

end_time := clock_timestamp();
duration := end_time - start_time;
RAISE NOTICE '============================================';
RAISE NOTICE 'TOTAL LOADING TIME : %', duration;
RAISE NOTICE '============================================';

EXCEPTION WHEN OTHERS THEN
GET STACKED DIAGNOSTICS
		v_state = RETURNED_SQLSTATE,
		v_message = MESSAGE_TEXT,
		v_detail = PG_EXCEPTION_DETAIL,
		v_hint = PG_EXCEPTION_HINT;

    RAISE NOTICE 'ERROR STATE  : %', v_state;
    RAISE NOTICE 'ERROR MSG    : %', v_message;
    RAISE NOTICE 'ERROR DETAIL : %', v_detail;
    RAISE NOTICE 'ERROR HINT   : %', v_hint;

END
$$;

CALL silver.load_silver()





