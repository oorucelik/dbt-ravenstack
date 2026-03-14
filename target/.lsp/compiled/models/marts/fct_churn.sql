-- fct_churn: Churn events fact table
-- Grain: one row per churn event

with churn as (
    select * from "main_staging"."stg_churn_events"
),

accounts as (
    select account_id, account_key
    from "main_marts"."dim_account"
)

select
    md5(cast(coalesce(cast(c.churn_event_id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT))  as churn_key,
    c.churn_event_id,

    -- Foreign keys
    a.account_key,

    -- Dates
    c.churn_date,

    -- Dimensions
    c.churn_reason,
    c.feedback_text,

    -- Flags
    c.had_preceding_upgrade,
    c.had_preceding_downgrade,
    c.is_reactivation,

    -- Measures
    c.refund_amount_usd,
    case when c.refund_amount_usd > 0 then true else false end as had_refund

from churn c
left join accounts a on c.account_id = a.account_id