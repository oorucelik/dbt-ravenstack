-- fct_subscriptions: Subscription fact table
-- Grain: one row per subscription event

with subscriptions as (
    select * from "main_staging"."stg_subscriptions"
),

accounts as (
    select account_id, account_key
    from "main_marts"."dim_account"
),

plans as (
    select plan_key, plan_tier, billing_frequency
    from "main_marts"."dim_plan"
)

select
    md5(cast(coalesce(cast(s.subscription_id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT))  as subscription_key,
    s.subscription_id,

    -- Foreign keys
    a.account_key,
    p.plan_key,

    -- Dates
    s.start_date,
    s.end_date,

    -- Measures
    s.seats,
    s.mrr_amount,
    s.arr_amount,
    case
        when s.end_date is not null
        then datediff('day', s.start_date, s.end_date)
        else datediff('day', s.start_date, current_date)
    end                                               as subscription_duration_days,

    -- Flags
    s.is_trial,
    s.is_upgrade,
    s.is_downgrade,
    s.has_churned,
    s.auto_renew,
    s.billing_frequency,

    -- Derived
    case
        when s.end_date is null and s.has_churned = false then 'active'
        when s.has_churned = true then 'churned'
        else 'expired'
    end                                               as subscription_status

from subscriptions s
left join accounts a on s.account_id = a.account_id
left join plans p
    on s.plan_tier = p.plan_tier
    and s.billing_frequency = p.billing_frequency