with
    transacoes as (
        select
            fk_conta
            , fk_cliente
            , fk_agencia
            , fk_colaborador
            , cast(ts_transacao as date) as data_transacao
            , tipo_trasacao
            , valor_transacao
        from {{ ref('int_fato_transacoes') }}
    )

    , dim_datas as (
        select
            pk_data
            , data_completa
        from {{ ref('int_dimensao_datas') }}
    )

    , range_de_transacoes as (
        select 
            fk_conta
            , fk_cliente
            , fk_agencia
            , fk_colaborador
            , min(data_transacao) as data_primeira_transacao
            , max(data_transacao) as data_ultima_transacao
        from transacoes
        group by 
            fk_conta
            , fk_cliente
            , fk_agencia
            , fk_colaborador
    )

    , gerando_todos_os_dias as (
        select
            range_de_transacoes.fk_conta
            , range_de_transacoes.fk_cliente
            , range_de_transacoes.fk_agencia
            , range_de_transacoes.fk_colaborador
            , dim_datas.pk_Data as fk_data
            , dim_datas.data_completa
        from range_de_transacoes
        cross join dim_datas
        where dim_datas.data_completa between 
            range_de_transacoes.data_primeira_transacao 
            and range_de_transacoes.data_ultima_transacao
    )

    , aggregado_transacoes as (
        select 
            fk_conta
            , data_transacao
            , tipo_trasacao
            , sum(valor_transacao) as transacao_total
        from transacoes
        group by 
            fk_conta
            , data_transacao
            , tipo_trasacao
    )

    , joined as (
        select
            gerando_todos_os_dias.fk_conta
            , gerando_todos_os_dias.fk_cliente
            , gerando_todos_os_dias.fk_agencia
            , gerando_todos_os_dias.fk_colaborador
            , gerando_todos_os_dias.fk_data
            , gerando_todos_os_dias.data_completa
            , coalesce(aggregado_transacoes.tipo_trasacao, 'Sem Movimento') as tipo_trasacao
            , coalesce(aggregado_transacoes.transacao_total, 0.0) as transacao_total
        from gerando_todos_os_dias
        left join aggregado_transacoes on 
            gerando_todos_os_dias.fk_conta = aggregado_transacoes.fk_conta
            and gerando_todos_os_dias.data_completa = aggregado_transacoes.data_transacao
    )

    , criar_chave as (
        select
            {{ dbt_utils.generate_surrogate_key([
                'fk_conta'
                , 'fk_data'
                , 'tipo_trasacao'
            ]) }} as sk_agg_transacoes_diarias
            , *
        from joined
    )

select *
from criar_chave
