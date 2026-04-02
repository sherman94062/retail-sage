{{ config(materialized='table') }}

with sales as (
    select * from {{ ref('int_sales_unified') }}
),

dates as (
    select * from {{ ref('stg_tpcds__date_dim') }}
)

select
    s.*,
    d.d_date,
    d.d_year,
    d.d_quarter_name,
    d.d_moy as d_month,
    d.d_dow as d_day_of_week,
    d.d_holiday as is_holiday,
    d.d_weekend as is_weekend
from sales s
left join dates d on s.sold_date_sk = d.d_date_sk
