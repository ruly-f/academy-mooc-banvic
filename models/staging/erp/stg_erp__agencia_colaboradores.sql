with
    fonte_colaborador_agencia as (
        select *
        from {{ source('erp', 'colaborador_agencia') }}
    )
    , renamed as (
        select *
        from fonte_colaborador_agencia
    )

select *
from renamed
