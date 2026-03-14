-- dim_date: Date dimension generated from the range of subscription data
-- Grain: one row per calendar day

with recursive date_spine as (
    select date '2023-01-01' as date_day
    union all
    select (date_day + interval '1 day')::date
    from date_spine
    where date_day < date '2025-01-01'
),

enriched as (
    select
        date_day::date                                as date_day,
        extract(year from date_day)::int              as year,
        extract(month from date_day)::int             as month,
        extract(quarter from date_day)::int           as quarter,
        extract(dow from date_day)::int               as day_of_week,
        strftime(date_day::date, '%B')                as month_name,
        strftime(date_day::date, '%A')                as day_name,
        case
            when extract(dow from date_day) in (0, 6) then true
            else false
        end                                           as is_weekend
    from date_spine
)

select
    {{ dbt_utils.generate_surrogate_key(['date_day']) }}  as date_key,
    *
from enriched
