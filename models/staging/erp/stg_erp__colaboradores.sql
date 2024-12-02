with
    fonte_colaboradores as (
        select *
        from {{ source('erp', 'colaboradores') }}
    )
    
    , renamed as (
        select *
        from fonte_colaboradores
    )

select *
from renamed
