-- fct_churn: Churn events fact table
-- Grain: one row per churn event

with churn as (
    select * from {{ ref('stg_churn_events') }}
),

accounts as (
    select account_id, account_key
    from {{ ref('dim_account') }}
)

select
    COUNT(c.churn_event_id) as churn_event_count,
    c.churn_reason

from churn c
left join accounts a
    on c.account_id = a.account_id
group by 
    churn_reason
order by
    churn_event_count desc

