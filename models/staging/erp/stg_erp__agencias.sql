with
    fonte_agencias as (
        select *
        from {{ source('erp', 'agencias') }}
    )

    , renomeado as (
        select
            cast(COD_AGENCIA as int) as pk_agencia
            , cast(NOME as varchar) as nome_agencia
            , cast(ENDERECO as varchar) as endereco_agencia
            , cast(CIDADE as varchar) as cidade_agencia
            , cast(UF as varchar) as uf_agencia
            , cast(DATA_ABERTURA as date) as data_de_abertura
            , cast(TIPO_AGENCIA as varchar) as tipo_agencia
        from fonte_agencias
    )

select *
from renomeado
