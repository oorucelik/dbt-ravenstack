-- dim_plan: Subscription plan dimension
-- Grain: one row per unique (plan_tier, billing_frequency) combination

with plans as (
    select distinct
        plan_tier,
        billing_frequency
    from "main_staging"."stg_subscriptions"
    where plan_tier is not null
)

select
    md5(cast(coalesce(cast(plan_tier as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(billing_frequency as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT))  as plan_key,
    plan_tier,
    billing_frequency,
    case plan_tier
        when 'Basic'      then 1
        when 'Pro'         then 2
        when 'Enterprise'  then 3
        else 0
    end as plan_tier_rank
from plans