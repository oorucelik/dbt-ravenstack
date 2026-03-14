with source as (
    select * from {{ ref('ravenstack_subscriptions') }}
),

renamed as (
    select
        subscription_id,
        account_id,
        cast(start_date as date) as start_date,
        cast(end_date as date) as end_date,
        plan_tier,
        seats,
        mrr_amount,
        arr_amount,
        cast(is_trial as boolean) as is_trial,
        cast(upgrade_flag as boolean) as is_upgrade,
        cast(downgrade_flag as boolean) as is_downgrade,
        cast(churn_flag as boolean) as has_churned,
        billing_frequency,
        cast(auto_renew_flag as boolean) as auto_renew,
        current_timestamp as _loaded_at
    from source
    where subscription_id is not null
)

select * from renamed
