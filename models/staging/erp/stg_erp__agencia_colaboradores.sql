with
    fonte_colaborador_agencia as (
        select *
        from {{ source('erp', 'colaborador_agencia') }}
    )
    , renamed as (
        select
            cod_colaborador as fk_colaborador
            , cod_agencia as fk_agencia
        from fonte_colaborador_agencia
    )

select *
from renamed
