{{ config(materialized='table') }}

select * from {{ ref('int_customer_profile') }}
