-- fct_feature_usage: Feature usage fact table
-- Grain: one row per daily feature usage log

with usage as (
    select * from "main_staging"."stg_feature_usage"
),

subscriptions as (
    select subscription_id, subscription_key, account_key
    from "main_marts"."fct_subscriptions"
)

select
    md5(cast(coalesce(cast(u.usage_id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT))  as usage_key,
    u.usage_id,

    -- Foreign keys
    s.subscription_key,
    s.account_key,

    -- Dimensions
    u.usage_date,
    u.feature_name,
    u.is_beta_feature,

    -- Measures
    u.usage_count,
    u.usage_duration_secs,
    round(u.usage_duration_secs / 60.0, 2)            as usage_duration_mins,
    u.error_count,
    case when u.error_count > 0 then true else false end as had_errors

from usage u
left join subscriptions s on u.subscription_id = s.subscription_id