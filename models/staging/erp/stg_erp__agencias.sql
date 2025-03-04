with
    fonte_agencias as (
        select *
        from {{ source('erp', 'agencias') }}
    )
    
    , renamed as (
        select
            cod_agencia as pk_agencia
            , cod_agencia as numero_agencia
            , nome as nome_agencia
            , endereco as endereco_agencia
            , cidade as cidade_agencia
            , uf as uf_agencia
            , cast(data_abertura as date) as data_abertura_agencia
            , tipo_agencia
        from fonte_agencias
    )

select *
from renamed
