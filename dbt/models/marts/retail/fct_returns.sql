{{ config(materialized='table') }}

with returns as (
    select * from {{ ref('int_returns_unified') }}
),

dates as (
    select * from {{ ref('stg_tpcds__date_dim') }}
)

select
    r.*,
    d.d_date,
    d.d_year,
    d.d_quarter_name,
    d.d_moy as d_month,
    d.d_dow as d_day_of_week
from returns r
left join dates d on r.returned_date_sk = d.d_date_sk
