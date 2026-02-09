CREATE SCHEMA bronze;

CREATE SCHEMA silver;

CREATE SCHEMA gold;

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


CREATE TABLE bronze.customers_raw (
    customer_id   VARCHAR(20),
    name          VARCHAR(100),
    email         VARCHAR(150),
    phone         VARCHAR(20),
    city          VARCHAR(50),
    signup_date   VARCHAR(50),
    updated_at    VARCHAR(50)
);

CREATE TABLE order_items_raw (
    order_id     VARCHAR(20),
    product_id   VARCHAR(20),
    quantity     VARCHAR(20),
    unit_price   VARCHAR(50)
);

CREATE TABLE payments_raw (
    payment_id      VARCHAR(20),
    order_id        VARCHAR(20),
    payment_mode    VARCHAR(20),
    payment_amount  VARCHAR(50),
    payment_status  VARCHAR(30),
    payment_ts      VARCHAR(50)
);

CREATE TABLE shipments_raw (
    shipment_id      VARCHAR(20),
    order_id         VARCHAR(20),
    shipped_date     VARCHAR(50),
    delivered_date   VARCHAR(50),
    carrier          VARCHAR(50)
);


