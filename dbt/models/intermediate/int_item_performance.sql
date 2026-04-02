{{
    config(
        materialized='table'
    )
}}

-- Item-level sales and return aggregations across all channels

with sales as (
    select * from {{ ref('int_sales_unified') }}
),

returns as (
    select * from {{ ref('int_returns_unified') }}
),

items as (
    select * from {{ ref('stg_tpcds__item') }}
),

sales_agg as (
    select
        item_sk,
        channel,
        count(*) as total_sales_txns,
        sum(quantity) as total_quantity_sold,
        sum(ext_sales_price) as total_sales_amount,
        sum(net_profit) as total_net_profit,
        avg(sales_price) as avg_sales_price
    from sales
    group by item_sk, channel
),

returns_agg as (
    select
        item_sk,
        channel,
        count(*) as total_return_txns,
        sum(return_quantity) as total_quantity_returned,
        sum(return_amt) as total_return_amount,
        sum(net_loss) as total_return_loss
    from returns
    group by item_sk, channel
)

select
    i.i_item_sk,
    i.i_item_id,
    i.i_item_desc,
    i.i_category,
    i.i_class,
    i.i_brand,
    i.i_manufact,
    i.i_current_price,
    s.channel,
    s.total_sales_txns,
    s.total_quantity_sold,
    s.total_sales_amount,
    s.total_net_profit,
    s.avg_sales_price,
    coalesce(r.total_return_txns, 0) as total_return_txns,
    coalesce(r.total_quantity_returned, 0) as total_quantity_returned,
    coalesce(r.total_return_amount, 0) as total_return_amount,
    coalesce(r.total_return_loss, 0) as total_return_loss,
    case
        when s.total_quantity_sold > 0
        then coalesce(r.total_quantity_returned, 0)::float / s.total_quantity_sold
        else 0
    end as return_rate

from sales_agg s
inner join items i on s.item_sk = i.i_item_sk
left join returns_agg r on s.item_sk = r.item_sk and s.channel = r.channel
