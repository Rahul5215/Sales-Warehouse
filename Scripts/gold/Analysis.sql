/*
Advanced Analytical Queries (Gold Layer)

This section contains advanced business analytics queries built on top of the Gold layer star schema. These queries demonstrate how the warehouse supports real-world reporting, KPI monitoring, and performance analysis using fact and dimension tables.

The analysis includes:

📈 Revenue trends (daily, monthly, city-wise)

> Top-performing products by revenue (overall and month-wise)

> Customer Lifetime Value (CLV) and customer segmentation

> First-order vs repeat-order behavior analysis

> Payment success and failure rate tracking

> Carrier delivery performance evaluation

> Order–shipment mismatch detection

> Revenue contribution percentage by product

> Time-based analysis (day-of-week, month, year breakdown)

The queries leverage advanced SQL techniques such as window functions (DENSE_RANK, SUM() OVER()), CTEs, multi-table joins across fact and dimension tables, and percentage calculations.

This layer demonstrates how the dimensional model enables efficient aggregation, behavioral insights, and business-focused analytics directly from the warehouse.
*/

select
*
from gold.fact_orders

select
*
from gold.fact_order_items

select
*
from gold.dim_customers

select
*
from gold.dim_products

--Total Orders Per Day
select
extract(day from order_ts),
count(order_id) 
from gold.fact_orders
group by extract(day from order_ts)
order by extract(day from order_ts)

--Daily revenue trend
select
order_ts::date,
sum(order_amount) as daily_revenue
from gold.fact_orders
group by order_ts::date
order by order_ts::date asc

--Orders by city
select
c.city,
count(o.order_id) as total_orders
from gold.fact_orders o
left join gold.dim_customers c
on c.customer_id = o.customer_id
group by c.city

--delivery performance by carrier
select
distinct
carrier,
count(order_id) over(partition by carrier)
from gold.fact_orders
where is_delivered is true

--Payment success rate
select
payment_status,
count(*)
from gold.fact_payments
group by payment_status

--top five products by revenue
with cte as(
select
product_id,
sum(revenue) as total_revenue
from gold.fact_order_items
group by product_id
)
,cte2 as
(
select
*,
dense_rank() over(order by total_revenue desc) as ranking
from cte
)
select
*
from cte2 
where ranking < 6

--average order value
select
avg(order_amount) as avg_order_value
from gold.fact_orders

--revenue by month
select
extract(month from order_ts) as months,
sum(order_amount) as total_revenue
from gold.fact_orders
group by extract(month from order_ts)
order by extract(month from order_ts) 

--orders vs payment missmatch
select
o.order_id
from gold.fact_orders o
left join gold.fact_payments p
on o.order_id = p.order_id
and p.payment_status = 'SUCCESS'
where p.order_id is null

--Orders without shipment
SELECT o.order_id
FROM gold.fact_orders o
LEFT JOIN silver.shipments s
ON o.order_id = s.order_id
WHERE s.order_id IS NULL;

select * from gold.fact_orders

select
order_id
from gold.fact_orders
where is_shipped is false


--CLV
select
c.customer_id,
c.city,
count(distinct o.order_id) as order_count,
sum(order_amount) as total_revnue,
avg(order_amount) as avg_order_value
from gold.fact_orders o
join gold.dim_customers c
on o.customer_id = c.customer_id
group by c.customer_id,c.city
order by sum(order_amount) desc

--revenue contribution per product
with cte as(
select
product_id,
sum(revenue) as revenue
from gold.fact_order_items
group by product_id
)
,cte2 as(
select
*,
sum(revenue) over() as total_revenue
from cte
)
select
*,
concat(round((revenue * 100 / total_revenue),2),'%') as contribution
from cte2


--first order vs repeat order analysis
WITH first_order AS (
  SELECT 
    customer_id,
    MIN(order_ts) AS first_order_ts
  FROM gold.fact_orders
  GROUP BY customer_id
)
SELECT 
  f.customer_id,
  COUNT(CASE WHEN o.order_ts = f.first_order_ts THEN 1 END) AS first_orders,
  COUNT(CASE WHEN o.order_ts > f.first_order_ts THEN 1 END) AS repeat_orders
FROM gold.fact_orders o
JOIN first_order f
ON o.customer_id = f.customer_id
GROUP BY f.customer_id;


--Top 3 products per months.
with cte as(
select
extract(month from o.order_ts) as months,
oi.product_id,
sum(oi.revenue) as total_sales
from gold.fact_orders o
join gold.fact_order_items oi
on oi.order_id = o.order_id
group by extract(month from o.order_ts),oi.product_id
)
,cte2 as(
select 
*,
dense_rank() over(partition by months order by total_sales desc) as ranking
from cte
)
select
*
from cte2
where ranking < 4

--Payment failure rate by day
SELECT 
  payment_times,
  COUNT(*) AS total_payments,
  SUM(CASE WHEN payment_status = 'FAILED' THEN 1 ELSE 0 END) AS failed_payments,
  ROUND(100.0 *
     SUM(CASE WHEN payment_status = 'FAILED' THEN 1 ELSE 0 END)
     / COUNT(*), 2) AS failure_rate_pct
FROM gold.fact_payments
GROUP BY payment_times
ORDER BY payment_times;


--Time gap between order and delivery
select
order_id,
shipped_date,
delivered_date,
(delivered_date - shipped_date) as delivery_days
from gold.fact_orders
where is_delivered is true

--Worst performing carrier
with cte as(
select 
carrier,
count(order_id) as total_orders,
sum(case when is_delivered then 1 else 0 end) as delivered_orders
from gold.fact_orders
group by carrier
)
select
*,
concat((delivered_orders * 100 / total_orders),'%') as deilbery_success_rate
from cte


--Top three performing products of each month per year.
with cte as(
select
extract(year from o.order_ts) as years,
extract(month from o.order_ts) as months,
oi.product_id,
p.product_name,
sum(oi.revenue) as total_revenue
from gold.fact_order_items oi
left join gold.fact_orders o
on o.order_id = oi.order_id
left join gold.dim_products p
on oi.product_id = p.product_id
group by extract(year from o.order_ts), extract(month from o.order_ts), oi.product_id, p.product_name
)
, cte2 as(
select 
*,
dense_rank() over(partition by years, months order by total_revenue desc) as ranking
from cte
)
select
*
from cte2
where ranking <= 3

--Orders by day-of-week
select
to_char(order_ts, 'day') as weekdays,
count(order_id) as total_orders,
sum(order_amount) as total_revenue
from gold.fact_orders
group by to_char(order_ts, 'day')
order by to_char(order_ts, 'day')

--Revenue by city + month
select
extract(month from o.order_ts) as months,
c.city,
sum(order_amount) as total_revenue
from gold.fact_orders o
left join gold.dim_customers c
on o.customer_id = c.customer_id
group by extract(month from o.order_ts), c.city

--Orders per customer bucket
with cte as(
select 
customer_id,
count(order_id) as orders
from gold.fact_orders
group by customer_id
order by customer_id
)
,cte2 as(
select
*,
case when orders > 10 then 'Loyal Customers'
     when orders < 2  then 'One Time Buyer'
	 else 'Regular Customers'
	 end as customer_segment
from cte
)
select
customer_segment,
count(*) as number_of_customers
from cte2
group by customer_segment






call bronze.load_bronze()
call silver.load_silver()
call gold.load_gold()


select *
from gold.fact_orders


