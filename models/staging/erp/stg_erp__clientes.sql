with
    fonte_clientes as (
        select *
        from {{ source('erp', 'clientes') }}
    )
    , renamed as (
        select *
        from fonte_clientes
    )

select *
from renamed
