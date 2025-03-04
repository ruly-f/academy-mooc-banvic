with
    transacoes as (
        select
            pk_transacao
            , fk_conta
            , numero_transacao
            , ts_transacao
            , nome_transacao
            , tipo_trasacao
            , valor_transacao
        from {{ ref('stg_erp__transacoes') }}
    )

    , contas as (
        select 
            pk_conta
            , fk_cliente
            , fk_agencia
            , fk_colaborador
        from {{ ref('int_fato_contas') }}
    )

    , joined as (
        select
            transacoes.pk_transacao
            , transacoes.fk_conta
            , contas.fk_cliente
            , contas.fk_agencia
            , contas.fk_colaborador
            , transacoes.numero_transacao
            , transacoes.ts_transacao
            , transacoes.nome_transacao
            , transacoes.tipo_trasacao
            , transacoes.valor_transacao
        from transacoes
        left join contas on transacoes.fk_conta = contas.pk_conta
    )

select *
from joined
