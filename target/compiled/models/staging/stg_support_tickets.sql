with source as (
    select * from "ravenstack"."main_raw"."ravenstack_support_tickets"
),

renamed as (
    select
        ticket_id,
        account_id,
        cast(submitted_at as date)                   as submitted_date,
        cast(closed_at as timestamp)                 as closed_at,
        resolution_time_hours,
        priority,
        first_response_time_minutes,
        satisfaction_score,
        cast(escalation_flag as boolean)             as is_escalated,
        current_timestamp                            as _loaded_at
    from source
    where ticket_id is not null
)

select * from renamed