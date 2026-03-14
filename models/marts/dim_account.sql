-- dim_account: Company-level dimension table
-- Grain: one row per account

with accounts as (
    select * from {{ ref('stg_accounts') }}
),

-- Enrich with aggregated support & churn stats
support_stats as (
    select
        account_id,
        count(*) as total_tickets,
        avg(satisfaction_score) as avg_satisfaction_score,
        sum(case when is_escalated then 1 else 0 end) as escalated_tickets
    from {{ ref('stg_support_tickets') }}
    group by account_id
),

churn_stats as (
    select
        account_id,
        count(*) as total_churn_events,
        max(churn_date) as last_churn_date
    from {{ ref('stg_churn_events') }}
    group by account_id
)

select
    {{ dbt_utils.generate_surrogate_key(['a.account_id']) }}  as account_key,
    a.account_id,
    a.account_name,
    a.industry,
    a.country,
    a.signup_date,
    a.referral_source,
    a.plan_tier as current_plan_tier,
    a.seats as current_seats,
    a.is_trial,
    a.has_churned,

    -- Support enrichment
    coalesce(s.total_tickets, 0) as lifetime_tickets,
    round(s.avg_satisfaction_score, 2) as avg_satisfaction_score,
    coalesce(s.escalated_tickets, 0) as escalated_tickets,

    -- Churn enrichment
    coalesce(c.total_churn_events, 0) as lifetime_churn_events,
    c.last_churn_date

from accounts a
left join support_stats s on a.account_id = s.account_id
left join churn_stats c on a.account_id = c.account_id
