with date_dimension as (
    {{ dbt_date.get_date_dimension("2023-01-01", "2025-01-01") }}
),
final as (
    select
        {{ dbt_utils.generate_surrogate_key(['date_day']) }} as date_key,
        date_day,
        year_number as year,
        month_of_year as month,
        quarter_of_year as quarter,
        day_of_week,
        month_name,
        -- dbt-date provides 'is_weekend' out of the box (boolean)
        is_weekend
    from date_dimension
)

select * from final