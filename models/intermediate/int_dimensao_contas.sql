with
    contas as (
        select
            pk_conta
            , numero_conta
            , tipo_conta
            , ts_abertura_conta
        from {{ ref('stg_erp__contas') }}
    )

select *
from contas
