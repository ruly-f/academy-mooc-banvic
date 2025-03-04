with 
    range_datas as (
        {{ 
            dbt_utils.date_spine(
                start_date = "cast('2010-01-01' as date)"
                , end_date = "cast('2023-12-31' as date)"
                , datepart = "day"
            ) 
        }}
    )

    , criar_colunas as (
        select
            date_day as pk_data
            , extract(year from date_day) as ano
            , extract(month from date_day) as mes
            , extract(day from date_day) as dia
            , extract(quarter from date_day) as trimestre
            , to_char(date_day, 'yyyy-mm-dd') as data_completa
            , extract(dow from date_day) as dia_da_semana
            , case 
                when extract(dow from date_day) in (0, 6) then true
                else false
            end as is_weekend
        from range_datas
    )

select *
from criar_colunas
