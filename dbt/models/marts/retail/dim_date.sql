{{ config(materialized='table') }}

select * from {{ ref('stg_tpcds__date_dim') }}
