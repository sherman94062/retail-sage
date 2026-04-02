with source as (
    select * from {{ source('tpcds', 'promotion') }}
),

renamed as (
    select * from source
)

select * from renamed
