{{
    config(
        materialized='table'
    )
}}

-- Unify all three return channels into a single table

with store_returns as (
    select
        sr_returned_date_sk as returned_date_sk,
        sr_return_time_sk as return_time_sk,
        sr_item_sk as item_sk,
        sr_customer_sk as customer_sk,
        sr_store_sk as store_sk,
        sr_ticket_number as order_number,
        sr_return_quantity as return_quantity,
        sr_return_amt as return_amt,
        sr_return_tax as return_tax,
        sr_net_loss as net_loss,
        sr_reason_sk as reason_sk,
        'store' as channel
    from {{ ref('stg_tpcds__store_returns') }}
),

catalog_returns as (
    select
        cr_returned_date_sk as returned_date_sk,
        cast(null as integer) as return_time_sk,
        cr_item_sk as item_sk,
        cr_returning_customer_sk as customer_sk,
        cast(null as integer) as store_sk,
        cr_order_number as order_number,
        cr_return_quantity as return_quantity,
        cr_return_amount as return_amt,
        cr_return_tax as return_tax,
        cr_net_loss as net_loss,
        cr_reason_sk as reason_sk,
        'catalog' as channel
    from {{ ref('stg_tpcds__catalog_returns') }}
),

web_returns as (
    select
        wr_returned_date_sk as returned_date_sk,
        cast(null as integer) as return_time_sk,
        wr_item_sk as item_sk,
        wr_returning_customer_sk as customer_sk,
        cast(null as integer) as store_sk,
        wr_order_number as order_number,
        wr_return_quantity as return_quantity,
        wr_return_amt as return_amt,
        wr_return_tax as return_tax,
        wr_net_loss as net_loss,
        wr_reason_sk as reason_sk,
        'web' as channel
    from {{ ref('stg_tpcds__web_returns') }}
)

select * from store_returns
union all
select * from catalog_returns
union all
select * from web_returns
