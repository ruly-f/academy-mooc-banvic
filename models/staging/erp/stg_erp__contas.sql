with
    fonte_contas as (
        select *
        from {{ source('erp', 'contas') }}
    )
    
    , renamed as (
        select *
        from fonte_contas
    )

select *
from renamed
