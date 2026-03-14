-- created_at: 2026-03-14T19:48:04.799886100+00:00
-- finished_at: 2026-03-14T19:48:04.808056900+00:00
-- elapsed: 8ms
-- outcome: success
-- dialect: duckdb
-- node_id: not available
-- query_id: not available
-- desc: Get table schema
DESCRIBE "ravenstack"."main_marts"."fct_subscriptions";
-- created_at: 2026-03-14T19:48:04.799886100+00:00
-- finished_at: 2026-03-14T19:48:04.808056900+00:00
-- elapsed: 8ms
-- outcome: success
-- dialect: duckdb
-- node_id: not available
-- query_id: not available
-- desc: Get table schema
DESCRIBE "ravenstack"."main_staging"."stg_feature_usage";
-- created_at: 2026-03-14T19:48:05.335579100+00:00
-- finished_at: 2026-03-14T19:48:05.346811200+00:00
-- elapsed: 11ms
-- outcome: success
-- dialect: duckdb
-- node_id: not available
-- query_id: not available
-- desc: dbt run query
select * from (-- fct_feature_usage: Feature usage fact table
-- Grain: one row per daily feature usage log

with usage as (
    select * from "ravenstack"."main_staging"."stg_feature_usage"
),

subscriptions as (
    select subscription_id, subscription_key, account_key
    from "ravenstack"."main_marts"."fct_subscriptions"
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
) limit 1000;
