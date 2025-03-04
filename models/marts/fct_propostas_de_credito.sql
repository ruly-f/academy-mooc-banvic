with
    fct_transacoes as (
        select *
        from {{ ref('int_fato_propostas_de_credito') }}
    )

select *
from fct_transacoes
