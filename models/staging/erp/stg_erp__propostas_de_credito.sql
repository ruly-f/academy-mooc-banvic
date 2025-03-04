with
    fonte_propostas_credito as (
        select *
        from {{ source('erp', 'propostas_credito') }}
    )
    
    , renamed as (
        select
            cod_proposta as pk_proposta
            , cod_cliente as fk_cliente
            , cod_colaborador as fk_colaborador
            , cod_proposta as numero_proposta
            , cast(data_entrada_proposta as timestamp) as ts_entrada_proposta
            , cast(taxa_juros_mensal as numeric(12,5)) as taxa_juros_mensal 
            , cast(valor_proposta as numeric(28,2)) as valor_proposta 
            , cast(valor_financiamento as numeric(28,2)) as valor_financiamento
            , cast(valor_entrada as numeric(28,2)) as valor_entrada
            , cast(valor_prestacao as numeric(28,2)) as valor_prestacao
            , cast(quantidade_parcelas as int) as quantidade_parcelas
            , cast(carencia as int) as carencia
            , status_proposta
        from fonte_propostas_credito
    )

select *
from renamed
