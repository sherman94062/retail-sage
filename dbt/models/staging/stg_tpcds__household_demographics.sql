with source as (
    select * from {{ source('tpcds', 'household_demographics') }}
),

renamed as (
    select * from source
)

select * from renamed
