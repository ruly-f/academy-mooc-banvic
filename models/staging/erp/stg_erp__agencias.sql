with
    fonte_agencias as (
        select *
        from {{ source('erp', 'agencias') }}
    )
    
    , renamed as (
        select *
        from fonte_agencias
    )

select *
from renamed
