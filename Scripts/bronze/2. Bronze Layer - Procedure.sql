/* ===============================================================
   PROCEDURE: bronze.load_bronze()
   LAYER: Bronze (Raw Ingestion Layer)

   DESCRIPTION:
   This procedure loads raw CSV data into Bronze layer tables.
   It performs:
   - Table truncation (Full refresh strategy)
   - Bulk load using COPY
   - Execution time tracking
   - Structured logging
   - Error diagnostics handling

   LOAD STRATEGY:
   Full refresh (TRUNCATE + COPY)

   =============================================================== */

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
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
v_state TEXT;
v_message TEXT;
v_hint TEXT;
v_detail TEXT;
BEGIN
start_time := clock_timestamp();
RAISE NOTICE '============================================';
RAISE NOTICE 'LOADING BRONZE LAYER';
RAISE NOTICE '============================================';

------------------------------------------------------
-- ORDERS
------------------------------------------------------

RAISE NOTICE '--------------------------------------------';
RAISE NOTICE 'LOADING TABLE : bronze.orders_raw';

t_start_time := clock_timestamp();
RAISE NOTICE 'TRUNCATING TABLE : bronze.orders_raw';
TRUNCATE TABLE bronze.orders_raw;

RAISE NOTICE 'INSERTING DATA INTO : bronze.orders_raw';
COPY bronze.orders_raw
FROM 'C:\Sales Warehouse\Sales Data Sets\orders_raw.csv'
CSV HEADER;
t_end_time := clock_timestamp();
t_duration := t_end_time - t_start_time;
RAISE NOTICE 'LOADING TIME FOR TABLE bronze.orders_raw: %',t_duration;

------------------------------------------------------
-- CUSTOMERS
------------------------------------------------------
RAISE NOTICE '--------------------------------------------';
RAISE NOTICE 'LOADING TABLE : bronze.customers_raw';

t_start_time := clock_timestamp();
RAISE NOTICE 'TRUNCATING TABLE : bronze.customers_raw';
TRUNCATE TABLE bronze.customers_raw;

RAISE NOTICE 'INSERTING DATA INTO : bronze.customers_raw';
COPY bronze.customers_raw
FROM 'C:\Sales Warehouse\Sales Data Sets\customers_raw.csv'
CSV HEADER;
t_end_time := clock_timestamp();
t_duration := t_end_time - t_start_time;
RAISE NOTICE 'LOADING TIME FOR bronze.customers_raw: %',t_duration;

------------------------------------------------------
-- ORDER_ITEMS
------------------------------------------------------
RAISE NOTICE '--------------------------------------------';
RAISE NOTICE 'LOADING TABLE : bronze.order_items';

t_start_time := clock_timestamp();
RAISE NOTICE 'TRUNCATING TABLE : bronze.order_items';
TRUNCATE TABLE bronze.order_items;

RAISE NOTICE 'INSERTING DATA INTO : bronze.order_items';
COPY bronze.order_items
FROM 'C:\Sales Warehouse\Sales Data Sets\order_items.csv'
CSV HEADER;
t_end_time := clock_timestamp();
t_duration := t_end_time - t_start_time;
RAISE NOTICE 'LOADING TIME FOR bronze.order_items_raw: %',t_duration;

------------------------------------------------------
-- PAYMENTS
------------------------------------------------------
RAISE NOTICE '--------------------------------------------';
RAISE NOTICE 'LOADING TABLE : bronze.payments_raw';

t_start_time := clock_timestamp();
RAISE NOTICE 'TRUNCATING TABLE : bronze.payments_raw';
TRUNCATE TABLE bronze.payments_raw;

RAISE NOTICE 'INSERTING DATA INTO : bronze.payments_raw';
COPY bronze.payments_raw
FROM 'C:\Sales Warehouse\Sales Data Sets\payments_raw.csv'
CSV HEADER;
t_end_time := clock_timestamp();
t_duration := t_end_time - t_start_time;
RAISE NOTICE 'LOADING TIME FOR bronze.payments_raw: %',t_duration;

------------------------------------------------------
-- SHIPMENTS
------------------------------------------------------
RAISE NOTICE '--------------------------------------------';
RAISE NOTICE 'LOADING TABLE : bronze.shipments_raw';

t_start_time := clock_timestamp();

RAISE NOTICE 'TRUNCATING TABLE : bronze.shipments_raw';
TRUNCATE TABLE bronze.shipments_raw;

RAISE NOTICE 'INSERTING DATA INTO : bronze.shipments_raw';
COPY bronze.shipments_raw
FROM 'C:\Sales Warehouse\Sales Data Sets\shipments_raw.csv'
CSV HEADER;
t_end_time := clock_timestamp();
t_duration := t_end_time - t_start_time;

RAISE NOTICE 'LOADING TIME FOR bronze.shipments_raw: %',t_duration;

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

RAISE NOTICE 'ERROR STATE   :%', v_state;
RAISE NOTICE 'ERROR MESSAGE :%', v_message;
RAISE NOTICE 'ERROR DETAIL  :%', v_detail;
RAISE NOTICE 'ERROR HINT 	:%', v_hint;


END
$$

CALL bronze.load_bronze()


