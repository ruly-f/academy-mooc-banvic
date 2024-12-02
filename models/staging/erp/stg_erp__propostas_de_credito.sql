with
    fonte_propostas_credito as (
        select *
        from {{ source('erp', 'propostas_credito') }}
    )
    
    , renamed as (
        select *
        from fonte_propostas_credito
    )

select *
from renamed
