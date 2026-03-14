with source as (
    select * from {{ ref('ravenstack_feature_usage') }}
),

renamed as (
    select
        usage_id,
        subscription_id,
        cast(usage_date as date)                     as usage_date,
        feature_name,
        usage_count,
        usage_duration_secs,
        error_count,
        cast(is_beta_feature as boolean)             as is_beta_feature,
        current_timestamp                            as _loaded_at
    from source
    where usage_id is not null
)

select * from renamed
