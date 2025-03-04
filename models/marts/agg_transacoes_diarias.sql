with
    agg_transacoes as (
        select *
        from {{ ref('int_agg_transacoes_diarias') }}
    )

select *
from agg_transacoes
