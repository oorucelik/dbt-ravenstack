with source as (
    select * from "main_raw"."ravenstack_churn_events"
),

renamed as (
    select
        churn_event_id,
        account_id,
        cast(churn_date as date) as churn_date,
        reason_code as churn_reason,
        refund_amount_usd,
        cast(preceding_upgrade_flag as boolean) as had_preceding_upgrade,
        cast(preceding_downgrade_flag as boolean) as had_preceding_downgrade,
        cast(is_reactivation as boolean) as is_reactivation,
        nullif(feedback_text, '') as feedback_text,
        current_timestamp as _loaded_at
    from source
    where churn_event_id is not null
)

select * from renamed