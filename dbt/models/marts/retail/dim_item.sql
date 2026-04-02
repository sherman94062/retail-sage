{{ config(materialized='table') }}

select * from {{ ref('stg_tpcds__item') }}
