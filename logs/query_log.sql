-- created_at: 2026-03-14T12:48:51.172581400+00:00
-- finished_at: 2026-03-14T12:48:51.180147800+00:00
-- elapsed: 7ms
-- outcome: success
-- dialect: duckdb
-- node_id: not available
-- query_id: not available
-- desc: Get table schema
DESCRIBE "ravenstack"."main_raw"."ravenstack_support_tickets";
-- created_at: 2026-03-14T12:48:51.533247500+00:00
-- finished_at: 2026-03-14T12:48:51.537896700+00:00
-- elapsed: 4ms
-- outcome: success
-- dialect: duckdb
-- node_id: not available
-- query_id: not available
-- desc: dbt run query
select * from (with source as (
    select * from "ravenstack"."main_raw"."ravenstack_support_tickets"
),

renamed as (
    select
        ticket_id,
        account_id,
        cast(submitted_at as date) as submitted_date,
        cast(closed_at as timestamp) as closed_at,
        resolution_time_hours,
        priority,
        first_response_time_minutes,
        satisfaction_score,
        cast(escalation_flag as boolean) as is_escalated,
        current_timestamp as _loaded_at
    from source
    where ticket_id is not null
)

select * from renamed
) limit 1000;
