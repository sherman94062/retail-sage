{{ config(materialized='table') }}

with sales as (
    select * from {{ ref('fct_sales') }}
)

select
    d_date,
    d_year,
    d_month,
    channel,
    count(*) as transaction_count,
    sum(quantity) as total_quantity,
    sum(ext_sales_price) as gross_sales,
    sum(net_paid) as net_sales,
    sum(net_profit) as net_profit,
    sum(coupon_amt) as total_coupons,
    avg(sales_price) as avg_selling_price,
    count(distinct customer_sk) as unique_customers
from sales
where d_date is not null
group by d_date, d_year, d_month, channel
