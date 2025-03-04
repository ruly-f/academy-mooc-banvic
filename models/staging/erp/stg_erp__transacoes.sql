with
    fonte_transacoes as (
        select *
        from {{ source('erp', 'transacoes') }}
    )
    
    , renamed as (
        select
            cod_transacao as pk_transacao
            , num_conta as fk_conta
            , cod_transacao as numero_transacao
            , cast(data_transacao as timestamp) ts_transacao
            , nome_transacao
            , case 
                when valor_transacao > 0 then 'Crédito'
                when valor_transacao < 0 then 'Débito'
                else null 
            end as tipo_trasacao
            , cast(valor_transacao as numeric(28,2)) as valor_transacao
        from fonte_transacoes
    )

select *
from renamed
