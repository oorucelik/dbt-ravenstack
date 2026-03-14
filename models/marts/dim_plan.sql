-- dim_plan: Subscription plan dimension
-- Grain: one row per unique (plan_tier, billing_frequency) combination

with plans as (
    select distinct
        plan_tier,
        billing_frequency
    from {{ ref('stg_subscriptions') }}
    where plan_tier is not null
)

select
    {{ dbt_utils.generate_surrogate_key(['plan_tier', 'billing_frequency']) }}  as plan_key,
    plan_tier,
    billing_frequency,
    case plan_tier
        when 'Basic'      then 1
        when 'Pro'         then 2
        when 'Enterprise'  then 3
        else 0
    end as plan_tier_rank
from plans
