with
    fonte_transacoes as (
        select *
        from {{ source('erp', 'transacoes') }}
    )
    
    , renamed as (
        select *
        from fonte_transacoes
    )

select *
from renamed
