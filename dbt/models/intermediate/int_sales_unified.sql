{{
    config(
        materialized='table'
    )
}}

-- Unify all three sales channels into a single table

with store_sales as (
    select
        ss_sold_date_sk as sold_date_sk,
        ss_sold_time_sk as sold_time_sk,
        ss_item_sk as item_sk,
        ss_customer_sk as customer_sk,
        ss_store_sk as store_sk,
        cast(null as integer) as warehouse_sk,
        ss_promo_sk as promo_sk,
        ss_ticket_number as order_number,
        ss_quantity as quantity,
        ss_wholesale_cost as wholesale_cost,
        ss_list_price as list_price,
        ss_sales_price as sales_price,
        ss_ext_sales_price as ext_sales_price,
        ss_ext_wholesale_cost as ext_wholesale_cost,
        ss_ext_list_price as ext_list_price,
        ss_ext_tax as ext_tax,
        ss_coupon_amt as coupon_amt,
        ss_net_paid as net_paid,
        ss_net_paid_inc_tax as net_paid_inc_tax,
        ss_net_profit as net_profit,
        'store' as channel
    from {{ ref('stg_tpcds__store_sales') }}
),

catalog_sales as (
    select
        cs_sold_date_sk as sold_date_sk,
        cs_sold_time_sk as sold_time_sk,
        cs_item_sk as item_sk,
        cs_bill_customer_sk as customer_sk,
        cast(null as integer) as store_sk,
        cs_warehouse_sk as warehouse_sk,
        cs_promo_sk as promo_sk,
        cs_order_number as order_number,
        cs_quantity as quantity,
        cs_wholesale_cost as wholesale_cost,
        cs_list_price as list_price,
        cs_sales_price as sales_price,
        cs_ext_sales_price as ext_sales_price,
        cs_ext_wholesale_cost as ext_wholesale_cost,
        cs_ext_list_price as ext_list_price,
        cs_ext_tax as ext_tax,
        cs_coupon_amt as coupon_amt,
        cs_net_paid as net_paid,
        cs_net_paid_inc_tax as net_paid_inc_tax,
        cs_net_profit as net_profit,
        'catalog' as channel
    from {{ ref('stg_tpcds__catalog_sales') }}
),

web_sales as (
    select
        ws_sold_date_sk as sold_date_sk,
        ws_sold_time_sk as sold_time_sk,
        ws_item_sk as item_sk,
        ws_bill_customer_sk as customer_sk,
        cast(null as integer) as store_sk,
        ws_warehouse_sk as warehouse_sk,
        ws_promo_sk as promo_sk,
        ws_order_number as order_number,
        ws_quantity as quantity,
        ws_wholesale_cost as wholesale_cost,
        ws_list_price as list_price,
        ws_sales_price as sales_price,
        ws_ext_sales_price as ext_sales_price,
        ws_ext_wholesale_cost as ext_wholesale_cost,
        ws_ext_list_price as ext_list_price,
        ws_ext_tax as ext_tax,
        ws_coupon_amt as coupon_amt,
        ws_net_paid as net_paid,
        ws_net_paid_inc_tax as net_paid_inc_tax,
        ws_net_profit as net_profit,
        'web' as channel
    from {{ ref('stg_tpcds__web_sales') }}
)

select * from store_sales
union all
select * from catalog_sales
union all
select * from web_sales
