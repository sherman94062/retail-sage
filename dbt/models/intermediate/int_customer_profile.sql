{{
    config(
        materialized='table'
    )
}}

-- Enriched customer profile joining demographics and address

with customers as (
    select * from {{ ref('stg_tpcds__customer') }}
),

addresses as (
    select * from {{ ref('stg_tpcds__customer_address') }}
),

demographics as (
    select * from {{ ref('stg_tpcds__customer_demographics') }}
),

household as (
    select * from {{ ref('stg_tpcds__household_demographics') }}
),

income as (
    select * from {{ ref('stg_tpcds__income_band') }}
)

select
    c.c_customer_sk,
    c.c_customer_id,
    c.c_first_name,
    c.c_last_name,
    c.c_birth_year,
    c.c_birth_country,
    c.c_email_address,
    c.c_preferred_cust_flag,

    -- Address
    a.ca_street_number,
    a.ca_street_name,
    a.ca_city,
    a.ca_county,
    a.ca_state,
    a.ca_zip,
    a.ca_country,
    a.ca_gmt_offset,

    -- Demographics
    d.cd_gender,
    d.cd_marital_status,
    d.cd_education_status,
    d.cd_credit_rating,
    d.cd_dep_count,
    d.cd_dep_employed_count,
    d.cd_dep_college_count,
    d.cd_purchase_estimate,

    -- Household
    h.hd_buy_potential,
    h.hd_dep_count as hd_dep_count,
    h.hd_vehicle_count,

    -- Income band
    ib.ib_lower_bound as income_lower,
    ib.ib_upper_bound as income_upper

from customers c
left join addresses a on c.c_current_addr_sk = a.ca_address_sk
left join demographics d on c.c_current_cdemo_sk = d.cd_demo_sk
left join household h on c.c_current_hdemo_sk = h.hd_demo_sk
left join income ib on h.hd_income_band_sk = ib.ib_income_band_sk
