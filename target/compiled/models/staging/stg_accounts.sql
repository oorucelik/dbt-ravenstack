with source as (
    select * from "ravenstack"."main_raw"."ravenstack_accounts"
),

renamed as (
    select
        account_id,
        account_name,
        industry,
        country,
        cast(signup_date as date)                   as signup_date,
        referral_source,
        plan_tier,
        seats,
        cast(is_trial as boolean)                   as is_trial,
        cast(churn_flag as boolean)                  as has_churned,
        current_timestamp                            as _loaded_at
    from source
    where account_id is not null
)

select * from renamed