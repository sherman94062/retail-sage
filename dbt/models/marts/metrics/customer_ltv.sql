{{ config(materialized='table') }}

with sales as (
    select * from {{ ref('fct_sales') }}
    where customer_sk is not null
),

customer_sales as (
    select
        customer_sk,
        min(d_date) as first_purchase_date,
        max(d_date) as last_purchase_date,
        count(distinct d_date) as purchase_days,
        count(*) as total_transactions,
        sum(ext_sales_price) as lifetime_gross_sales,
        sum(net_paid) as lifetime_net_sales,
        sum(net_profit) as lifetime_net_profit,
        avg(ext_sales_price) as avg_transaction_value,
        count(distinct channel) as channels_used
    from sales
    group by customer_sk
)

select
    c.*,
    cust.c_first_name,
    cust.c_last_name,
    cust.ca_state,
    cust.cd_gender,
    cust.cd_education_status,
    cust.income_lower,
    cust.income_upper
from customer_sales c
left join {{ ref('dim_customer') }} cust on c.customer_sk = cust.c_customer_sk
