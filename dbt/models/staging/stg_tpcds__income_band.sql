with source as (
    select * from {{ source('tpcds', 'income_band') }}
),

renamed as (
    select * from source
)

select * from renamed
