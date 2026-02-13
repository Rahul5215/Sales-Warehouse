/*🥈 Silver Layer – Data Quality Validation & Cleaning Logic

This script implements structured data quality validation and cleansing logic as part of the Silver layer transformation process. After raw data ingestion into the Bronze layer, this step ensures that data is standardized, validated, and prepared for analytical modeling in the Gold layer.

The script performs:

- Duplicate removal using ROW_NUMBER() to retain the most recent records

- Null handling and default value assignment

- Pattern and format validation using regular expressions

- Referential integrity checks across related tables

- Categorical standardization using UPPER(), INITCAP(), and trimming

- Temporal anomaly detection for unrealistic dates

- Data type validation and safe casting to enforce structured schema rules

This layer acts as a controlled transformation stage where raw operational data is converted into reliable, analytics-ready datasets while preserving traceability and data integrity.
*/
---------------------------------------
--For Column : order_id
---------------------------------------
--checking for duplicates in order_id
select
order_id,
count(order_id)
from bronze.orders_raw
group by order_id
having count(order_id) > 1

--removing duplicates from order_id
select
*
from
(
select
*,
row_number() over(partition by order_id order by updated_at desc) as ranking
from bronze.orders_raw
)
where ranking = 1

--checking consistency of order_id
select
*
from bronze.orders_raw
where order_id not like 'ORD%'

---------------------------------------
--For Column : order_ts
---------------------------------------
--Formatig date
select
*,
order_ts::TIMESTAMP
FROM bronze.orders_raw

SELECT order_id, order_ts::TIMESTAMP
FROM bronze.orders_raw
WHERE order_ts ILIKE '%AM%'
   OR order_ts ILIKE 'Jan%'
   OR order_ts ILIKE '%Jan%';


--checking for future dates
select
order_ts
FROM bronze.orders_raw
WHERE order_ts::DATE > CURRENT_DATE

--checking for past dates
select
order_ts
FROM bronze.orders_raw
WHERE order_ts::DATE < CURRENT_DATE - INTERVAL '100 YEARS'


---------------------------------------
--For Column : order_ts
---------------------------------------
select
*
FROM bronze.orders_raw
where customer_id is null

--to handle nulls in customer_id
INSERT INTO silver.customers_silver
VALUES (
'C0000','Unknown Customer','unknown@na','NA','NA',NULL,CURRENT_TIMESTAMP
);

---------------------------------------
--For Column : order_status
---------------------------------------
--cheking consistency of order_status column
select
distinct
order_status
from bronze.orders_raw

--improving consistency
select
distinct
upper(order_status)
from bronze.orders_raw

--Validation check
select
*
from 
(
select
*,
upper(order_status) as order_status_new
from bronze.orders_raw
)
where order_status_new = 'CANCELLED' and order_amount::int > 0

---------------------------------------
--For Column : order_amount
---------------------------------------
select
*
from bronze.orders_raw
where order_amount is null or trim(order_amount) = ''

---------------------------------------
--For Column : currency
---------------------------------------
--Consistency checking
select
distinct 
currency
from bronze.orders_raw

---------------------------------------
--For Column : source_system
---------------------------------------
--Consistency checking
select
distinct 
source_system
from bronze.orders_raw

---------------------------------------
--For Column : updated_at
---------------------------------------






--Final Table:-
SELECT
order_id,
NULLIF(order_ts, '')::TIMESTAMP AS order_ts,
COALESCE(customer_id, 'COOOO') AS customer_id, 
UPPER(order_status) AS order_status,
order_amount,
currency,
source_system,
updated_at
FROM
(
select
*,
row_number() over(partition by order_id order by updated_at desc) as ranking
from bronze.orders_raw
)
where ranking = 1

---------------------------------------
--For Column : customer_id
---------------------------------------
select *from bronze.customers_raw
--Checking for nulls
select
customer_id
from bronze.customers_raw
where customer_id is null

--checking for duplicates and removing duplicates.
select
*
from
(
select
*,
row_number() over(partition by customer_id order by updated_at) as ranking
from bronze.customers_raw
)
where ranking = 1

---------------------------------------
--For Column : name
---------------------------------------
--checking for nulls
select
name
from bronze.customers_raw
where name is null

---------------------------------------
--For Column : emial
---------------------------------------
--Check for nulls
select
email
from bronze.customers_raw
where email is null

--Check valid domain structure
select
email
from bronze.customers_raw
where email not similar to '%@%.%'

--Check for @
select
email
from bronze.customers_raw
where email not similar to '%@%'

--check for exactly one @
select
email
from bronze.customers_raw
where length(email) - length(replace(email,'@','')) <> 1

--check for spaces in email
select
email
from bronze.customers_raw
where email not similar to '% %'

---------------------------------------
--For Column : phone
---------------------------------------
--Check for invalid phone number and nulls.
SELECT phone 
FROM bronze.customers_raw
WHERE phone IS NULL OR phone !~ '^[0-9]{10}$';

---------------------------------------
--For Column : city
---------------------------------------
--Check for consistency in city names.
select
distinct
city
FROM bronze.customers_raw

--Improving consistency of city names.
select
distinct
initcap(city)
FROM bronze.customers_raw

---------------------------------------
--For Column : signup_date
---------------------------------------
select
signup_date::date
from bronze.customers_raw

---------------------------------------
--For Column : signup_date
---------------------------------------
--Check for nulls
select
updated_at
from bronze.customers_raw
where updated_at is null

--Check for future dates
select
updated_at::timestamp
from bronze.customers_raw
where updated_at::timestamp > current_timestamp

--check for invalid dates
select
updated_at::TIMESTAMP
from bronze.customers_raw




--Final Query:-
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
signup_date::date as signup_date,
updated_at::TIMESTAMP AS updated_at
from
(
select
*,
row_number() over(partition by customer_id order by updated_at desc) as ranking
from bronze.customers_raw
)
where ranking = 1


select *from bronze.order_items_raw
---------------------------------------
--For Column : order_id
---------------------------------------
--Check for nulls and empty values.
select
order_id
from bronze.order_items_raw
where order_id is null or trim(order_id) = ''

--Check for consistency.
select
order_id
from bronze.order_items_raw
where order_id not similar to 'ORD-%'

---------------------------------------
--For Column : product_id
---------------------------------------
--Check for nulls and empty values.
select
product_id
from bronze.order_items_raw
where product_id is null or trim(product_id) = ''

--Check for unwanted spaces
select
product_id
from bronze.order_items_raw
where product_id != trim(product_id)


--Consistency check
select
product_id
from bronze.order_items_raw
where product_id not similar to 'P1%'

--Check for invalid characters.
select
product_id
from bronze.order_items_raw
where product_id !~ '^P[0-9]+$'

--Check for duplicate rows for the same product inside the same order. 
select
order_id,
product_id,
count(product_id)
from bronze.order_items_raw
group by order_id,product_id
having count(product_id) > 1


select *from bronze.order_items_raw

---------------------------------------
--For Column : product_id
---------------------------------------
--Check for nulls and empty values.
select
quantity
from bronze.order_items_raw
where quantity is null or trim(quantity) = ''

--Consistency check
select
quantity
from bronze.order_items_raw
where quantity !~ '^[0-9]$'

--Improve consistency
select
case when trim(quantity) = 'two' then '2'
     when trim(quantity) = 'one' then '1'
	 else trim(quantity)
	 end as quantity
from bronze.order_items_raw

---------------------------------------
--For Column : unit_price
---------------------------------------
--Check for nulls and empty values.
select
unit_price
from bronze.order_items_raw
where unit_price is null or trim(unit_price) = ''

--Check for invalid values.
select
unit_price
from bronze.order_items_raw
where unit_price !~ '^[0-9]+(\.[0-9]+)?$';




--Final Query
select
order_id,
product_id,
case when trim(quantity) = 'two' then '2'
     when trim(quantity) = 'one' then '1'
	 else trim(quantity)
end as quantity,
unit_price
from bronze.order_items_raw


---------------------------------------
--For Table : payments_raw
---------------------------------------
select * from bronze.payments_raw
---------------------------------------
--For Column : payment_id
---------------------------------------
--Check for nulls and empty values.
select
payment_id
from bronze.payments_raw
where payment_id is null or trim(payment_id) = ''

--Check for unwanted spaces.
select
payment_id
from bronze.payments_raw
where payment_id != trim(payment_id)

--Consistency check.
select
payment_id
from bronze.payments_raw
where payment_id not similar to 'PAY-%'

--Inavalid values check.
select
payment_id
from bronze.payments_raw
where payment_id !~ '^PAY-[0-9]+$'

---------------------------------------
--For Column : order_id
---------------------------------------
--Check for nulls and empty values.
select
order_id
from bronze.payments_raw
where order_id is null or trim(order_id) = ''

--Check for unwanted spaces.
select
order_id
from bronze.payments_raw
where order_id != trim(order_id)

--Consistency check.
select
order_id
from bronze.payments_raw
where order_id not similar to 'ORD-%'

--Inavalid values check.
select
order_id
from bronze.payments_raw
where order_id !~ '^ORD-[0-9]+$'

--Check for payment order_ids that do NOT exist in orders_raw.
select
p.order_id
from bronze.payments_raw p
left join bronze.orders_raw o
on p.order_id = o.order_id
where o.order_id is null

---------------------------------------
--For Column : order_id
---------------------------------------
--Check for nulls and empty values.
select
payment_mode
from bronze.payments_raw
where payment_mode is null or trim(payment_mode) = ''

--Check for unwanted spaces.
select
payment_mode
from bronze.payments_raw
where payment_mode != trim(payment_mode)

--Cosnistency check
select
distinct
payment_mode
from bronze.payments_raw

---------------------------------------
--For Column : payment_amount
---------------------------------------
--Check for nulls and empty spaces.
select
payment_amount
from bronze.payments_raw
where payment_amount is null or trim(payment_amount) = ''

--Check for invalid values
select
payment_amount
from bronze.payments_raw
where payment_amount !~ '^[0-9]+(\.[0-9]+)?$'

---------------------------------------
--For Column : payment_status
---------------------------------------
--Check for nulls and empty values.
select
payment_status
from bronze.payments_raw
where payment_status is null or trim(payment_status) = ''

--Check for unwanted spaces.
select
payment_status
from bronze.payments_raw
where payment_status != trim(payment_status)

--Cosnistency check
select
distinct
payment_status
from bronze.payments_raw

---------------------------------------
--For Column : payment_ts
---------------------------------------
select *from bronze.payments_raw

--Check for nulls and empty values.
select
payment_ts
from bronze.payments_raw
where payment_ts is null or trim(payment_ts) = ''

--Check for unwanted spaces.
select
payment_ts
from bronze.payments_raw
where payment_ts != trim(payment_ts)

--Check for future dates
select
payment_ts
from bronze.payments_raw
where payment_ts::date > current_date

--Final Query
select
payment_id,
order_id,
payment_mode,
payment_amount,
payment_status,
payment_ts
from bronze.payments_raw

---------------------------------------
--For Table : shipments_raw
---------------------------------------

select *from bronze.shipments_raw

---------------------------------------
--For Column : shipment_id
---------------------------------------
--Check for nulls and empty values.
select
shipment_id
from bronze.shipments_raw
where shipment_id is null or trim(shipment_id) = ''

--Check for unwanted spaces.
select
shipment_id
from bronze.shipments_raw
where shipment_id != trim(shipment_id)

--Consistency check.
select
shipment_id
from bronze.shipments_raw
where shipment_id not similar to 'SHP-%'

--Check for invalid values
select
shipment_id
from bronze.shipments_raw
where shipment_id !~ '^SHP-[0-9]+$'

--Replacing invalid values
select
case when shipment_id like 'SHP%'
     or shipment_id like 'SH%'
	 or shipment_id like 'SP%'
	 then 'SHP-' || regexp_replace(shipment_id,'[^0-9]','','g')
     else shipment_id
end as shipment_id
from bronze.shipments_raw


---------------------------------------
--For Column : order_id
---------------------------------------
select *from bronze.shipments_raw

--Check for nulls and empty values.
select
order_id
from bronze.shipments_raw
where order_id is null or trim(order_id) = ''

--Check for unwanted spaces.
select
shipment_id
from bronze.shipments_raw
where order_id != trim(order_id)

--Consistency check.
select
order_id
from bronze.shipments_raw
where order_id not like 'ORD-%'

--Check for invalid values
select
order_id
from bronze.shipments_raw
where order_id !~ '^ORD-[0-9]+$'

--
select
s.order_id
from bronze.shipments_raw s
left join bronze.orders_raw o
on o.order_id = s.order_id
where o.order_id is null


---------------------------------------
--For Column : shipped_date
---------------------------------------
select *from bronze.shipments_raw

--Check for nulls and empty values.
select
*
from bronze.shipments_raw
where shipped_date is null or trim(shipped_date) = ''

--Check for unwanted spaces.
select
shipped_date
from bronze.shipments_raw
where shipped_date != trim(shipped_date)

--Check for future dates
select
shipped_date
from bronze.shipments_raw
where shipped_date::date > current_date


select
*
from bronze.shipments_raw
where shipped_date is null or delivered_date is null

---------------------------------------
--For Column : shipped_date
---------------------------------------
--Checking consistency
select
distinct
carrier
from bronze.shipments_raw

--Check for nulls and empty values.
select
*
from bronze.shipments_raw
where carrier is null or trim(carrier) = ''

--Check for unwanted spaces.
select
carrier
from bronze.shipments_raw
where carrier != trim(carrier)





--Final Query:-
SELECT
CASE WHEN shipment_id LIKE 'SHP%'
     OR shipment_id LIKE 'SH%'
	 OR shipment_id LIKE 'SP%'
	 THEN 'SHP-' || regexp_replace(shipment_id,'[^0-9]','','g')
     ELSE shipment_id
END AS shipment_id,
order_id,
shipped_date,
CASE WHEN shipped_date IS NOT NULL THEN TRUE
     ELSE FALSE
END AS is_shipped,
delivered_date,
CASE WHEN delivered_date IS NOT NULL THEN TRUE
     ELSE FALSE
END AS is_delivered,
carrier
FROM bronze.shipments_raw





