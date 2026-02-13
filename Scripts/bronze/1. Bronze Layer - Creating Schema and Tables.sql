/* ==============================================================
   PROJECT: Sales Data Warehouse
   LAYER: Bronze (Raw Ingestion Layer)

   DESCRIPTION:
   This script initializes the schema structure and raw tables 
   for the Bronze layer of the Sales Data Warehouse.

   The Bronze layer stores data exactly as received from source
   systems without any transformations.
   ============================================================== */


/* ==============================================================
   STEP 1: Create Warehouse Schemas
   --------------------------------------------------------------
   The warehouse follows Medallion Architecture:

   - Bronze → Raw source data (no transformations)
   - Silver → Cleaned and standardized data
   - Gold   → Business-ready analytics layer

   Schemas help logically separate processing stages.
   ============================================================== */

CREATE SCHEMA bronze;

CREATE SCHEMA silver;

CREATE SCHEMA gold;

/* ==============================================================
   BRONZE LAYER DESIGN PRINCIPLES
   --------------------------------------------------------------
   1. Store raw data exactly as received
   2. No business rules applied
   3. No datatype enforcement (all VARCHAR)
   4. Prevent load failures from bad formatting
   5. Maintain full audit traceability
   ============================================================== */


/* ==============================================================
   TABLE: bronze.orders_raw
   --------------------------------------------------------------
   Raw order-level transactional data.
   One row represents one order from source systems.
   ============================================================== */

DROP TABLE IF EXISTS bronze.orders_raw;
CREATE TABLE bronze.orders_raw (
    order_id        VARCHAR(20),
    order_ts        VARCHAR(50),
    customer_id     VARCHAR(20),
    order_status    VARCHAR(30),
    order_amount    VARCHAR(50),
    currency        VARCHAR(10),
    source_system   VARCHAR(20),
    updated_at      VARCHAR(50)
);

/* ==============================================================
   TABLE: bronze.customers_raw
   --------------------------------------------------------------
   Raw customer master data.
   Contains basic customer profile information.
   ============================================================== */

DROP TABLE IF EXISTS bronze.customers_raw;
CREATE TABLE bronze.customers_raw (
    customer_id   VARCHAR(20),
    name          VARCHAR(100),
    email         VARCHAR(150),
    phone         VARCHAR(20),
    city          VARCHAR(50),
    signup_date   VARCHAR(50),
    updated_at    VARCHAR(50)
);

/* ==============================================================
   TABLE: bronze.order_items
   --------------------------------------------------------------
   Raw product-level details for each order.
   One order may have multiple items.
   ============================================================== */

DROP TABLE IF EXISTS bronze.order_items;
CREATE TABLE bronze.order_items (
    order_id     VARCHAR(20),
    product_id   VARCHAR(20),
	product_name VARCHAR(20),
	category     VARCHAR(20),
    quantity     VARCHAR(20),
    unit_price   VARCHAR(50)
);

/* ==============================================================
   TABLE: bronze.payments_raw
   --------------------------------------------------------------
   Raw payment transaction data.
   An order may have one or multiple payment attempts.
   ============================================================== */

DROP TABLE IF EXISTS bronze.payments_raw;
CREATE TABLE bronze.payments_raw (
    payment_id      VARCHAR(20),
    order_id        VARCHAR(20),
    payment_mode    VARCHAR(20),
    payment_amount  VARCHAR(50),
    payment_status  VARCHAR(30),
    payment_ts      VARCHAR(50)
);

/* ==============================================================
   TABLE: bronze.shipments_raw
   --------------------------------------------------------------
   Raw shipment and delivery data.
   Used for logistics and delivery performance analysis.
   ============================================================== */

DROP TABLE IF EXISTS bronze.shipments_raw;
CREATE TABLE bronze.shipments_raw (
    shipment_id      VARCHAR(20),
    order_id         VARCHAR(20),
    shipped_date     VARCHAR(50),
    delivered_date   VARCHAR(50),
    carrier          VARCHAR(50)
);



