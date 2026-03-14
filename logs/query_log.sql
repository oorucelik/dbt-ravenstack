-- created_at: 2026-03-14T09:27:39.117063700+00:00
-- finished_at: 2026-03-14T09:27:39.118007300+00:00
-- elapsed: 943us
-- outcome: success
-- dialect: duckdb
-- node_id: not available
-- query_id: not available
-- desc: execute adapter call
/* {"app": "dbt", "connection_name": "", "dbt_version": "2.0.0", "profile_name": "dbt_ravenstack", "target_name": "dev"} */

    
    select schema_name
    from system.information_schema.schemata
    
    where lower(catalog_name) = '"ravenstack"'
    
  
  ;
-- created_at: 2026-03-14T09:27:39.118403600+00:00
-- finished_at: 2026-03-14T09:27:39.118730900+00:00
-- elapsed: 327us
-- outcome: success
-- dialect: duckdb
-- node_id: not available
-- query_id: not available
-- desc: execute adapter call
/* {"app": "dbt", "connection_name": "", "dbt_version": "2.0.0", "profile_name": "dbt_ravenstack", "target_name": "dev"} */

    
        select type from duckdb_databases()
        where lower(database_name)='ravenstack'
        and type='sqlite'
    
  ;
-- created_at: 2026-03-14T09:27:39.118913+00:00
-- finished_at: 2026-03-14T09:27:39.119083200+00:00
-- elapsed: 170us
-- outcome: success
-- dialect: duckdb
-- node_id: not available
-- query_id: not available
-- desc: execute adapter call
/* {"app": "dbt", "connection_name": "", "dbt_version": "2.0.0", "profile_name": "dbt_ravenstack", "target_name": "dev"} */

    
    
        create schema if not exists "ravenstack"."main_marts"
    ;
-- created_at: 2026-03-14T09:27:39.119271+00:00
-- finished_at: 2026-03-14T09:27:39.119595+00:00
-- elapsed: 324us
-- outcome: success
-- dialect: duckdb
-- node_id: not available
-- query_id: not available
-- desc: execute adapter call
/* {"app": "dbt", "connection_name": "", "dbt_version": "2.0.0", "profile_name": "dbt_ravenstack", "target_name": "dev"} */

    
        select type from duckdb_databases()
        where lower(database_name)='ravenstack'
        and type='sqlite'
    
  ;
-- created_at: 2026-03-14T09:27:39.119717900+00:00
-- finished_at: 2026-03-14T09:27:39.119851600+00:00
-- elapsed: 133us
-- outcome: success
-- dialect: duckdb
-- node_id: not available
-- query_id: not available
-- desc: execute adapter call
/* {"app": "dbt", "connection_name": "", "dbt_version": "2.0.0", "profile_name": "dbt_ravenstack", "target_name": "dev"} */

    
    
        create schema if not exists "ravenstack"."main_raw"
    ;
-- created_at: 2026-03-14T09:27:39.120191+00:00
-- finished_at: 2026-03-14T09:27:39.120558800+00:00
-- elapsed: 367us
-- outcome: success
-- dialect: duckdb
-- node_id: not available
-- query_id: not available
-- desc: execute adapter call
/* {"app": "dbt", "connection_name": "", "dbt_version": "2.0.0", "profile_name": "dbt_ravenstack", "target_name": "dev"} */

    
        select type from duckdb_databases()
        where lower(database_name)='ravenstack'
        and type='sqlite'
    
  ;
-- created_at: 2026-03-14T09:27:39.120680400+00:00
-- finished_at: 2026-03-14T09:27:39.120811500+00:00
-- elapsed: 131us
-- outcome: success
-- dialect: duckdb
-- node_id: not available
-- query_id: not available
-- desc: execute adapter call
/* {"app": "dbt", "connection_name": "", "dbt_version": "2.0.0", "profile_name": "dbt_ravenstack", "target_name": "dev"} */

    
    
        create schema if not exists "ravenstack"."main_staging"
    ;
-- created_at: 2026-03-14T09:27:39.141241500+00:00
-- finished_at: 2026-03-14T09:27:39.152141900+00:00
-- elapsed: 10ms
-- outcome: success
-- dialect: duckdb
-- node_id: seed.dbt_ravenstack.ravenstack_accounts
-- query_id: not available
-- desc: get_relation > list_relations call
SELECT table_catalog, table_schema, table_name, table_type FROM information_schema.tables WHERE table_schema = 'main_raw';
-- created_at: 2026-03-14T09:27:39.146217300+00:00
-- finished_at: 2026-03-14T09:27:39.152591900+00:00
-- elapsed: 6ms
-- outcome: success
-- dialect: duckdb
-- node_id: seed.dbt_ravenstack.ravenstack_support_tickets
-- query_id: not available
-- desc: get_relation > list_relations call
SELECT table_catalog, table_schema, table_name, table_type FROM information_schema.tables WHERE table_schema = 'main_raw';
-- created_at: 2026-03-14T09:27:39.141077300+00:00
-- finished_at: 2026-03-14T09:27:39.153883200+00:00
-- elapsed: 12ms
-- outcome: success
-- dialect: duckdb
-- node_id: seed.dbt_ravenstack.ravenstack_churn_events
-- query_id: not available
-- desc: get_relation > list_relations call
SELECT table_catalog, table_schema, table_name, table_type FROM information_schema.tables WHERE table_schema = 'main_raw';
-- created_at: 2026-03-14T09:27:39.153536400+00:00
-- finished_at: 2026-03-14T09:27:39.164292700+00:00
-- elapsed: 10ms
-- outcome: success
-- dialect: duckdb
-- node_id: seed.dbt_ravenstack.ravenstack_accounts
-- query_id: not available
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "seed.dbt_ravenstack.ravenstack_accounts", "profile_name": "dbt_ravenstack", "target_name": "dev"} */
truncate table "ravenstack"."main_raw"."ravenstack_accounts";
-- created_at: 2026-03-14T09:27:39.153871500+00:00
-- finished_at: 2026-03-14T09:27:39.165485800+00:00
-- elapsed: 11ms
-- outcome: success
-- dialect: duckdb
-- node_id: seed.dbt_ravenstack.ravenstack_support_tickets
-- query_id: not available
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "seed.dbt_ravenstack.ravenstack_support_tickets", "profile_name": "dbt_ravenstack", "target_name": "dev"} */
truncate table "ravenstack"."main_raw"."ravenstack_support_tickets";
-- created_at: 2026-03-14T09:27:39.162332600+00:00
-- finished_at: 2026-03-14T09:27:39.166934100+00:00
-- elapsed: 4ms
-- outcome: success
-- dialect: duckdb
-- node_id: seed.dbt_ravenstack.ravenstack_subscriptions
-- query_id: not available
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "seed.dbt_ravenstack.ravenstack_subscriptions", "profile_name": "dbt_ravenstack", "target_name": "dev"} */
truncate table "ravenstack"."main_raw"."ravenstack_subscriptions";
-- created_at: 2026-03-14T09:27:39.155763100+00:00
-- finished_at: 2026-03-14T09:27:39.168140700+00:00
-- elapsed: 12ms
-- outcome: success
-- dialect: duckdb
-- node_id: seed.dbt_ravenstack.ravenstack_churn_events
-- query_id: not available
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "seed.dbt_ravenstack.ravenstack_churn_events", "profile_name": "dbt_ravenstack", "target_name": "dev"} */
truncate table "ravenstack"."main_raw"."ravenstack_churn_events";
-- created_at: 2026-03-14T09:27:39.168211800+00:00
-- finished_at: 2026-03-14T09:27:39.206124400+00:00
-- elapsed: 37ms
-- outcome: success
-- dialect: duckdb
-- node_id: seed.dbt_ravenstack.ravenstack_subscriptions
-- query_id: not available
-- desc: add_query adapter call

          COPY "ravenstack"."main_raw"."ravenstack_subscriptions" FROM 'C:\BlogIdeas\dbt_ravenstack\seeds\ravenstack_subscriptions.csv' (FORMAT CSV, HEADER TRUE, DELIMITER ',')
        ;
-- created_at: 2026-03-14T09:27:39.166754100+00:00
-- finished_at: 2026-03-14T09:27:39.208606200+00:00
-- elapsed: 41ms
-- outcome: success
-- dialect: duckdb
-- node_id: seed.dbt_ravenstack.ravenstack_accounts
-- query_id: not available
-- desc: add_query adapter call

          COPY "ravenstack"."main_raw"."ravenstack_accounts" FROM 'C:\BlogIdeas\dbt_ravenstack\seeds\ravenstack_accounts.csv' (FORMAT CSV, HEADER TRUE, DELIMITER ',')
        ;
-- created_at: 2026-03-14T09:27:39.209554300+00:00
-- finished_at: 2026-03-14T09:27:39.213458800+00:00
-- elapsed: 3ms
-- outcome: success
-- dialect: duckdb
-- node_id: model.dbt_ravenstack.stg_subscriptions
-- query_id: not available
-- desc: get_relation > list_relations call
SELECT table_catalog, table_schema, table_name, table_type FROM information_schema.tables WHERE table_schema = 'main_staging';
-- created_at: 2026-03-14T09:27:39.170247600+00:00
-- finished_at: 2026-03-14T09:27:39.214991700+00:00
-- elapsed: 44ms
-- outcome: success
-- dialect: duckdb
-- node_id: seed.dbt_ravenstack.ravenstack_churn_events
-- query_id: not available
-- desc: add_query adapter call

          COPY "ravenstack"."main_raw"."ravenstack_churn_events" FROM 'C:\BlogIdeas\dbt_ravenstack\seeds\ravenstack_churn_events.csv' (FORMAT CSV, HEADER TRUE, DELIMITER ',')
        ;
-- created_at: 2026-03-14T09:27:39.216048600+00:00
-- finished_at: 2026-03-14T09:27:39.218385600+00:00
-- elapsed: 2ms
-- outcome: success
-- dialect: duckdb
-- node_id: model.dbt_ravenstack.stg_subscriptions
-- query_id: not available
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.dbt_ravenstack.stg_subscriptions", "profile_name": "dbt_ravenstack", "target_name": "dev"} */

  
  create view "ravenstack"."main_staging"."stg_subscriptions__dbt_tmp" as (
    with source as (
    select * from "ravenstack"."main_raw"."ravenstack_subscriptions"
),

renamed as (
    select
        subscription_id,
        account_id,
        cast(start_date as date)                    as start_date,
        cast(end_date as date)                      as end_date,
        plan_tier,
        seats,
        mrr_amount,
        arr_amount,
        cast(is_trial as boolean)                    as is_trial,
        cast(upgrade_flag as boolean)                as is_upgrade,
        cast(downgrade_flag as boolean)              as is_downgrade,
        cast(churn_flag as boolean)                   as has_churned,
        billing_frequency,
        cast(auto_renew_flag as boolean)             as auto_renew,
        current_timestamp                            as _loaded_at
    from source
    where subscription_id is not null
)

select * from renamed
  );
;
-- created_at: 2026-03-14T09:27:39.213267800+00:00
-- finished_at: 2026-03-14T09:27:39.218460700+00:00
-- elapsed: 5ms
-- outcome: success
-- dialect: duckdb
-- node_id: model.dbt_ravenstack.stg_accounts
-- query_id: not available
-- desc: get_relation > list_relations call
SELECT table_catalog, table_schema, table_name, table_type FROM information_schema.tables WHERE table_schema = 'main_staging';
-- created_at: 2026-03-14T09:27:39.220265200+00:00
-- finished_at: 2026-03-14T09:27:39.222243+00:00
-- elapsed: 1ms
-- outcome: success
-- dialect: duckdb
-- node_id: model.dbt_ravenstack.stg_subscriptions
-- query_id: not available
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.dbt_ravenstack.stg_subscriptions", "profile_name": "dbt_ravenstack", "target_name": "dev"} */
alter view "ravenstack"."main_staging"."stg_subscriptions" rename to "stg_subscriptions__dbt_backup";
-- created_at: 2026-03-14T09:27:39.221339800+00:00
-- finished_at: 2026-03-14T09:27:39.224253500+00:00
-- elapsed: 2ms
-- outcome: success
-- dialect: duckdb
-- node_id: model.dbt_ravenstack.stg_accounts
-- query_id: not available
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.dbt_ravenstack.stg_accounts", "profile_name": "dbt_ravenstack", "target_name": "dev"} */

  
  create view "ravenstack"."main_staging"."stg_accounts__dbt_tmp" as (
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
  );
;
-- created_at: 2026-03-14T09:27:39.223792600+00:00
-- finished_at: 2026-03-14T09:27:39.225970600+00:00
-- elapsed: 2ms
-- outcome: success
-- dialect: duckdb
-- node_id: model.dbt_ravenstack.stg_subscriptions
-- query_id: not available
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.dbt_ravenstack.stg_subscriptions", "profile_name": "dbt_ravenstack", "target_name": "dev"} */
alter view "ravenstack"."main_staging"."stg_subscriptions__dbt_tmp" rename to "stg_subscriptions";
-- created_at: 2026-03-14T09:27:39.225699400+00:00
-- finished_at: 2026-03-14T09:27:39.227103400+00:00
-- elapsed: 1ms
-- outcome: success
-- dialect: duckdb
-- node_id: model.dbt_ravenstack.stg_accounts
-- query_id: not available
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.dbt_ravenstack.stg_accounts", "profile_name": "dbt_ravenstack", "target_name": "dev"} */
alter view "ravenstack"."main_staging"."stg_accounts" rename to "stg_accounts__dbt_backup";
-- created_at: 2026-03-14T09:27:39.222838700+00:00
-- finished_at: 2026-03-14T09:27:39.228116800+00:00
-- elapsed: 5ms
-- outcome: success
-- dialect: duckdb
-- node_id: model.dbt_ravenstack.stg_churn_events
-- query_id: not available
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.dbt_ravenstack.stg_churn_events", "profile_name": "dbt_ravenstack", "target_name": "dev"} */

  
  create view "ravenstack"."main_staging"."stg_churn_events__dbt_tmp" as (
    with source as (
    select * from "ravenstack"."main_raw"."ravenstack_churn_events"
),

renamed as (
    select
        churn_event_id,
        account_id,
        cast(churn_date as date)                     as churn_date,
        reason_code                                  as churn_reason,
        refund_amount_usd,
        cast(preceding_upgrade_flag as boolean)      as had_preceding_upgrade,
        cast(preceding_downgrade_flag as boolean)    as had_preceding_downgrade,
        cast(is_reactivation as boolean)             as is_reactivation,
        nullif(feedback_text, '')                     as feedback_text,
        current_timestamp                            as _loaded_at
    from source
    where churn_event_id is not null
)

select * from renamed
  );
;
-- created_at: 2026-03-14T09:27:39.227841500+00:00
-- finished_at: 2026-03-14T09:27:39.228948500+00:00
-- elapsed: 1ms
-- outcome: success
-- dialect: duckdb
-- node_id: model.dbt_ravenstack.stg_accounts
-- query_id: not available
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.dbt_ravenstack.stg_accounts", "profile_name": "dbt_ravenstack", "target_name": "dev"} */
alter view "ravenstack"."main_staging"."stg_accounts__dbt_tmp" rename to "stg_accounts";
-- created_at: 2026-03-14T09:27:39.227079600+00:00
-- finished_at: 2026-03-14T09:27:39.230018300+00:00
-- elapsed: 2ms
-- outcome: success
-- dialect: duckdb
-- node_id: seed.dbt_ravenstack.ravenstack_feature_usage
-- query_id: not available
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "seed.dbt_ravenstack.ravenstack_feature_usage", "profile_name": "dbt_ravenstack", "target_name": "dev"} */
truncate table "ravenstack"."main_raw"."ravenstack_feature_usage";
-- created_at: 2026-03-14T09:27:39.228006100+00:00
-- finished_at: 2026-03-14T09:27:39.230975700+00:00
-- elapsed: 2ms
-- outcome: success
-- dialect: duckdb
-- node_id: model.dbt_ravenstack.stg_subscriptions
-- query_id: not available
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.dbt_ravenstack.stg_subscriptions", "profile_name": "dbt_ravenstack", "target_name": "dev"} */

      drop view if exists "ravenstack"."main_staging"."stg_subscriptions__dbt_backup" cascade
    ;
-- created_at: 2026-03-14T09:27:39.228957600+00:00
-- finished_at: 2026-03-14T09:27:39.231684100+00:00
-- elapsed: 2ms
-- outcome: success
-- dialect: duckdb
-- node_id: model.dbt_ravenstack.stg_churn_events
-- query_id: not available
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.dbt_ravenstack.stg_churn_events", "profile_name": "dbt_ravenstack", "target_name": "dev"} */
alter view "ravenstack"."main_staging"."stg_churn_events" rename to "stg_churn_events__dbt_backup";
-- created_at: 2026-03-14T09:27:39.230099500+00:00
-- finished_at: 2026-03-14T09:27:39.232937800+00:00
-- elapsed: 2ms
-- outcome: success
-- dialect: duckdb
-- node_id: model.dbt_ravenstack.stg_accounts
-- query_id: not available
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.dbt_ravenstack.stg_accounts", "profile_name": "dbt_ravenstack", "target_name": "dev"} */

      drop view if exists "ravenstack"."main_staging"."stg_accounts__dbt_backup" cascade
    ;
-- created_at: 2026-03-14T09:27:39.232681600+00:00
-- finished_at: 2026-03-14T09:27:39.255135200+00:00
-- elapsed: 22ms
-- outcome: success
-- dialect: duckdb
-- node_id: model.dbt_ravenstack.stg_churn_events
-- query_id: not available
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.dbt_ravenstack.stg_churn_events", "profile_name": "dbt_ravenstack", "target_name": "dev"} */
alter view "ravenstack"."main_staging"."stg_churn_events__dbt_tmp" rename to "stg_churn_events";
-- created_at: 2026-03-14T09:27:39.256647400+00:00
-- finished_at: 2026-03-14T09:27:39.257765300+00:00
-- elapsed: 1ms
-- outcome: success
-- dialect: duckdb
-- node_id: model.dbt_ravenstack.stg_churn_events
-- query_id: not available
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.dbt_ravenstack.stg_churn_events", "profile_name": "dbt_ravenstack", "target_name": "dev"} */

      drop view if exists "ravenstack"."main_staging"."stg_churn_events__dbt_backup" cascade
    ;
-- created_at: 2026-03-14T09:27:39.232736+00:00
-- finished_at: 2026-03-14T09:27:39.257826400+00:00
-- elapsed: 25ms
-- outcome: success
-- dialect: duckdb
-- node_id: model.dbt_ravenstack.dim_plan
-- query_id: not available
-- desc: get_relation > list_relations call
SELECT table_catalog, table_schema, table_name, table_type FROM information_schema.tables WHERE table_schema = 'main_marts';
-- created_at: 2026-03-14T09:27:39.260524800+00:00
-- finished_at: 2026-03-14T09:27:39.267387300+00:00
-- elapsed: 6ms
-- outcome: success
-- dialect: duckdb
-- node_id: model.dbt_ravenstack.dim_plan
-- query_id: not available
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.dbt_ravenstack.dim_plan", "profile_name": "dbt_ravenstack", "target_name": "dev"} */

  
    
    

    create  table
      "ravenstack"."main_marts"."dim_plan__dbt_tmp"
  
    as (
      -- dim_plan: Subscription plan dimension
-- Grain: one row per unique (plan_tier, billing_frequency) combination

with plans as (
    select distinct
        plan_tier,
        billing_frequency
    from "ravenstack"."main_staging"."stg_subscriptions"
    where plan_tier is not null
)

select
    md5(cast(coalesce(cast(plan_tier as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(billing_frequency as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT))  as plan_key,
    plan_tier,
    billing_frequency,
    case plan_tier
        when 'Basic'      then 1
        when 'Pro'         then 2
        when 'Enterprise'  then 3
        else 0
    end as plan_tier_rank
from plans
    );
  
  ;
-- created_at: 2026-03-14T09:27:39.268162+00:00
-- finished_at: 2026-03-14T09:27:39.268596600+00:00
-- elapsed: 434us
-- outcome: success
-- dialect: duckdb
-- node_id: model.dbt_ravenstack.dim_plan
-- query_id: not available
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.dbt_ravenstack.dim_plan", "profile_name": "dbt_ravenstack", "target_name": "dev"} */

    SELECT index_name
    FROM duckdb_indexes()
    WHERE schema_name = 'main_marts'
      AND table_name = 'dim_plan'
  ;
-- created_at: 2026-03-14T09:27:39.269140900+00:00
-- finished_at: 2026-03-14T09:27:39.269601800+00:00
-- elapsed: 460us
-- outcome: success
-- dialect: duckdb
-- node_id: model.dbt_ravenstack.dim_plan
-- query_id: not available
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.dbt_ravenstack.dim_plan", "profile_name": "dbt_ravenstack", "target_name": "dev"} */

    SELECT COUNT(*) as remaining_indexes
    FROM duckdb_indexes()
    WHERE schema_name = 'main_marts'
      AND table_name = 'dim_plan'
  ;
-- created_at: 2026-03-14T09:27:39.270399900+00:00
-- finished_at: 2026-03-14T09:27:39.271418200+00:00
-- elapsed: 1ms
-- outcome: success
-- dialect: duckdb
-- node_id: model.dbt_ravenstack.dim_plan
-- query_id: not available
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.dbt_ravenstack.dim_plan", "profile_name": "dbt_ravenstack", "target_name": "dev"} */
alter table "ravenstack"."main_marts"."dim_plan" rename to "dim_plan__dbt_backup";
-- created_at: 2026-03-14T09:27:39.272443600+00:00
-- finished_at: 2026-03-14T09:27:39.273781500+00:00
-- elapsed: 1ms
-- outcome: success
-- dialect: duckdb
-- node_id: model.dbt_ravenstack.dim_plan
-- query_id: not available
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.dbt_ravenstack.dim_plan", "profile_name": "dbt_ravenstack", "target_name": "dev"} */
alter table "ravenstack"."main_marts"."dim_plan__dbt_tmp" rename to "dim_plan";
-- created_at: 2026-03-14T09:27:39.275218100+00:00
-- finished_at: 2026-03-14T09:27:39.276204+00:00
-- elapsed: 985us
-- outcome: success
-- dialect: duckdb
-- node_id: model.dbt_ravenstack.dim_plan
-- query_id: not available
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.dbt_ravenstack.dim_plan", "profile_name": "dbt_ravenstack", "target_name": "dev"} */

      drop table if exists "ravenstack"."main_marts"."dim_plan__dbt_backup" cascade
    ;
-- created_at: 2026-03-14T09:27:39.278725500+00:00
-- finished_at: 2026-03-14T09:27:39.279364700+00:00
-- elapsed: 639us
-- outcome: success
-- dialect: duckdb
-- node_id: test.dbt_ravenstack.not_null_dim_plan_plan_key.d6964559f1
-- query_id: not available
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.dbt_ravenstack.not_null_dim_plan_plan_key.d6964559f1", "profile_name": "dbt_ravenstack", "target_name": "dev"} */

    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select plan_key
from "ravenstack"."main_marts"."dim_plan"
where plan_key is null



  
  
      
    ) dbt_internal_test;
-- created_at: 2026-03-14T09:27:39.279380600+00:00
-- finished_at: 2026-03-14T09:27:39.281523500+00:00
-- elapsed: 2ms
-- outcome: success
-- dialect: duckdb
-- node_id: test.dbt_ravenstack.unique_dim_plan_plan_key.03acfc39bf
-- query_id: not available
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.dbt_ravenstack.unique_dim_plan_plan_key.03acfc39bf", "profile_name": "dbt_ravenstack", "target_name": "dev"} */

    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    plan_key as unique_field,
    count(*) as n_records

from "ravenstack"."main_marts"."dim_plan"
where plan_key is not null
group by plan_key
having count(*) > 1



  
  
      
    ) dbt_internal_test;
-- created_at: 2026-03-14T09:27:39.231231300+00:00
-- finished_at: 2026-03-14T09:27:39.322511100+00:00
-- elapsed: 91ms
-- outcome: success
-- dialect: duckdb
-- node_id: seed.dbt_ravenstack.ravenstack_feature_usage
-- query_id: not available
-- desc: add_query adapter call

          COPY "ravenstack"."main_raw"."ravenstack_feature_usage" FROM 'C:\BlogIdeas\dbt_ravenstack\seeds\ravenstack_feature_usage.csv' (FORMAT CSV, HEADER TRUE, DELIMITER ',')
        ;
-- created_at: 2026-03-14T09:27:39.327694400+00:00
-- finished_at: 2026-03-14T09:27:39.329219300+00:00
-- elapsed: 1ms
-- outcome: success
-- dialect: duckdb
-- node_id: model.dbt_ravenstack.stg_feature_usage
-- query_id: not available
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.dbt_ravenstack.stg_feature_usage", "profile_name": "dbt_ravenstack", "target_name": "dev"} */

  
  create view "ravenstack"."main_staging"."stg_feature_usage__dbt_tmp" as (
    with source as (
    select * from "ravenstack"."main_raw"."ravenstack_feature_usage"
),

renamed as (
    select
        usage_id,
        subscription_id,
        cast(usage_date as date)                     as usage_date,
        feature_name,
        usage_count,
        usage_duration_secs,
        error_count,
        cast(is_beta_feature as boolean)             as is_beta_feature,
        current_timestamp                            as _loaded_at
    from source
    where usage_id is not null
)

select * from renamed
  );
;
-- created_at: 2026-03-14T09:27:39.330054600+00:00
-- finished_at: 2026-03-14T09:27:39.330877600+00:00
-- elapsed: 823us
-- outcome: success
-- dialect: duckdb
-- node_id: model.dbt_ravenstack.stg_feature_usage
-- query_id: not available
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.dbt_ravenstack.stg_feature_usage", "profile_name": "dbt_ravenstack", "target_name": "dev"} */
alter view "ravenstack"."main_staging"."stg_feature_usage" rename to "stg_feature_usage__dbt_backup";
-- created_at: 2026-03-14T09:27:39.331525+00:00
-- finished_at: 2026-03-14T09:27:39.332269600+00:00
-- elapsed: 744us
-- outcome: success
-- dialect: duckdb
-- node_id: model.dbt_ravenstack.stg_feature_usage
-- query_id: not available
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.dbt_ravenstack.stg_feature_usage", "profile_name": "dbt_ravenstack", "target_name": "dev"} */
alter view "ravenstack"."main_staging"."stg_feature_usage__dbt_tmp" rename to "stg_feature_usage";
-- created_at: 2026-03-14T09:27:39.333366300+00:00
-- finished_at: 2026-03-14T09:27:39.334173600+00:00
-- elapsed: 807us
-- outcome: success
-- dialect: duckdb
-- node_id: model.dbt_ravenstack.stg_feature_usage
-- query_id: not available
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.dbt_ravenstack.stg_feature_usage", "profile_name": "dbt_ravenstack", "target_name": "dev"} */

      drop view if exists "ravenstack"."main_staging"."stg_feature_usage__dbt_backup" cascade
    ;
-- created_at: 2026-03-14T09:27:39.167755400+00:00
-- finished_at: 2026-03-14T09:27:39.616595300+00:00
-- elapsed: 448ms
-- outcome: success
-- dialect: duckdb
-- node_id: seed.dbt_ravenstack.ravenstack_support_tickets
-- query_id: not available
-- desc: add_query adapter call

          COPY "ravenstack"."main_raw"."ravenstack_support_tickets" FROM 'C:\BlogIdeas\dbt_ravenstack\seeds\ravenstack_support_tickets.csv' (FORMAT CSV, HEADER TRUE, DELIMITER ',')
        ;
-- created_at: 2026-03-14T09:27:39.621766900+00:00
-- finished_at: 2026-03-14T09:27:39.623400700+00:00
-- elapsed: 1ms
-- outcome: success
-- dialect: duckdb
-- node_id: model.dbt_ravenstack.stg_support_tickets
-- query_id: not available
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.dbt_ravenstack.stg_support_tickets", "profile_name": "dbt_ravenstack", "target_name": "dev"} */

  
  create view "ravenstack"."main_staging"."stg_support_tickets__dbt_tmp" as (
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
  );
;
-- created_at: 2026-03-14T09:27:39.624126200+00:00
-- finished_at: 2026-03-14T09:27:39.625024200+00:00
-- elapsed: 898us
-- outcome: success
-- dialect: duckdb
-- node_id: model.dbt_ravenstack.stg_support_tickets
-- query_id: not available
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.dbt_ravenstack.stg_support_tickets", "profile_name": "dbt_ravenstack", "target_name": "dev"} */
alter view "ravenstack"."main_staging"."stg_support_tickets" rename to "stg_support_tickets__dbt_backup";
-- created_at: 2026-03-14T09:27:39.625625100+00:00
-- finished_at: 2026-03-14T09:27:39.626646200+00:00
-- elapsed: 1ms
-- outcome: success
-- dialect: duckdb
-- node_id: model.dbt_ravenstack.stg_support_tickets
-- query_id: not available
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.dbt_ravenstack.stg_support_tickets", "profile_name": "dbt_ravenstack", "target_name": "dev"} */
alter view "ravenstack"."main_staging"."stg_support_tickets__dbt_tmp" rename to "stg_support_tickets";
-- created_at: 2026-03-14T09:27:39.627814500+00:00
-- finished_at: 2026-03-14T09:27:39.628725400+00:00
-- elapsed: 910us
-- outcome: success
-- dialect: duckdb
-- node_id: model.dbt_ravenstack.stg_support_tickets
-- query_id: not available
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.dbt_ravenstack.stg_support_tickets", "profile_name": "dbt_ravenstack", "target_name": "dev"} */

      drop view if exists "ravenstack"."main_staging"."stg_support_tickets__dbt_backup" cascade
    ;
-- created_at: 2026-03-14T09:27:39.631280800+00:00
-- finished_at: 2026-03-14T09:27:39.641572200+00:00
-- elapsed: 10ms
-- outcome: success
-- dialect: duckdb
-- node_id: model.dbt_ravenstack.dim_account
-- query_id: not available
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.dbt_ravenstack.dim_account", "profile_name": "dbt_ravenstack", "target_name": "dev"} */

  
    
    

    create  table
      "ravenstack"."main_marts"."dim_account__dbt_tmp"
  
    as (
      -- dim_account: Company-level dimension table
-- Grain: one row per account

with accounts as (
    select * from "ravenstack"."main_staging"."stg_accounts"
),

-- Enrich with aggregated support & churn stats
support_stats as (
    select
        account_id,
        count(*)                                      as total_tickets,
        avg(satisfaction_score)                        as avg_satisfaction_score,
        sum(case when is_escalated then 1 else 0 end) as escalated_tickets
    from "ravenstack"."main_staging"."stg_support_tickets"
    group by account_id
),

churn_stats as (
    select
        account_id,
        count(*)                                      as total_churn_events,
        max(churn_date)                               as last_churn_date
    from "ravenstack"."main_staging"."stg_churn_events"
    group by account_id
)

select
    md5(cast(coalesce(cast(a.account_id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT))  as account_key,
    a.account_id,
    a.account_name,
    a.industry,
    a.country,
    a.signup_date,
    a.referral_source,
    a.plan_tier                                       as current_plan_tier,
    a.seats                                           as current_seats,
    a.is_trial,
    a.has_churned,

    -- Support enrichment
    coalesce(s.total_tickets, 0)                      as lifetime_tickets,
    round(s.avg_satisfaction_score, 2)                 as avg_satisfaction_score,
    coalesce(s.escalated_tickets, 0)                  as escalated_tickets,

    -- Churn enrichment
    coalesce(c.total_churn_events, 0)                 as lifetime_churn_events,
    c.last_churn_date

from accounts a
left join support_stats s on a.account_id = s.account_id
left join churn_stats c on a.account_id = c.account_id
    );
  
  ;
-- created_at: 2026-03-14T09:27:39.642240600+00:00
-- finished_at: 2026-03-14T09:27:39.642653100+00:00
-- elapsed: 412us
-- outcome: success
-- dialect: duckdb
-- node_id: model.dbt_ravenstack.dim_account
-- query_id: not available
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.dbt_ravenstack.dim_account", "profile_name": "dbt_ravenstack", "target_name": "dev"} */

    SELECT index_name
    FROM duckdb_indexes()
    WHERE schema_name = 'main_marts'
      AND table_name = 'dim_account'
  ;
-- created_at: 2026-03-14T09:27:39.643121900+00:00
-- finished_at: 2026-03-14T09:27:39.643483200+00:00
-- elapsed: 361us
-- outcome: success
-- dialect: duckdb
-- node_id: model.dbt_ravenstack.dim_account
-- query_id: not available
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.dbt_ravenstack.dim_account", "profile_name": "dbt_ravenstack", "target_name": "dev"} */

    SELECT COUNT(*) as remaining_indexes
    FROM duckdb_indexes()
    WHERE schema_name = 'main_marts'
      AND table_name = 'dim_account'
  ;
-- created_at: 2026-03-14T09:27:39.644192200+00:00
-- finished_at: 2026-03-14T09:27:39.645024900+00:00
-- elapsed: 832us
-- outcome: success
-- dialect: duckdb
-- node_id: model.dbt_ravenstack.dim_account
-- query_id: not available
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.dbt_ravenstack.dim_account", "profile_name": "dbt_ravenstack", "target_name": "dev"} */
alter table "ravenstack"."main_marts"."dim_account" rename to "dim_account__dbt_backup";
-- created_at: 2026-03-14T09:27:39.645832200+00:00
-- finished_at: 2026-03-14T09:27:39.646717+00:00
-- elapsed: 884us
-- outcome: success
-- dialect: duckdb
-- node_id: model.dbt_ravenstack.dim_account
-- query_id: not available
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.dbt_ravenstack.dim_account", "profile_name": "dbt_ravenstack", "target_name": "dev"} */
alter table "ravenstack"."main_marts"."dim_account__dbt_tmp" rename to "dim_account";
-- created_at: 2026-03-14T09:27:39.648811800+00:00
-- finished_at: 2026-03-14T09:27:39.649828900+00:00
-- elapsed: 1ms
-- outcome: success
-- dialect: duckdb
-- node_id: model.dbt_ravenstack.dim_account
-- query_id: not available
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.dbt_ravenstack.dim_account", "profile_name": "dbt_ravenstack", "target_name": "dev"} */

      drop table if exists "ravenstack"."main_marts"."dim_account__dbt_backup" cascade
    ;
-- created_at: 2026-03-14T09:27:39.652240300+00:00
-- finished_at: 2026-03-14T09:27:39.652743800+00:00
-- elapsed: 503us
-- outcome: success
-- dialect: duckdb
-- node_id: test.dbt_ravenstack.not_null_dim_account_account_key.591d147d42
-- query_id: not available
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.dbt_ravenstack.not_null_dim_account_account_key.591d147d42", "profile_name": "dbt_ravenstack", "target_name": "dev"} */

    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select account_key
from "ravenstack"."main_marts"."dim_account"
where account_key is null



  
  
      
    ) dbt_internal_test;
-- created_at: 2026-03-14T09:27:39.652341500+00:00
-- finished_at: 2026-03-14T09:27:39.653762400+00:00
-- elapsed: 1ms
-- outcome: success
-- dialect: duckdb
-- node_id: test.dbt_ravenstack.unique_dim_account_account_key.1ea0c20a30
-- query_id: not available
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.dbt_ravenstack.unique_dim_account_account_key.1ea0c20a30", "profile_name": "dbt_ravenstack", "target_name": "dev"} */

    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    account_key as unique_field,
    count(*) as n_records

from "ravenstack"."main_marts"."dim_account"
where account_key is not null
group by account_key
having count(*) > 1



  
  
      
    ) dbt_internal_test;
-- created_at: 2026-03-14T09:27:39.657660600+00:00
-- finished_at: 2026-03-14T09:27:39.662763400+00:00
-- elapsed: 5ms
-- outcome: success
-- dialect: duckdb
-- node_id: model.dbt_ravenstack.fct_churn
-- query_id: not available
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.dbt_ravenstack.fct_churn", "profile_name": "dbt_ravenstack", "target_name": "dev"} */

  
    
    

    create  table
      "ravenstack"."main_marts"."fct_churn__dbt_tmp"
  
    as (
      -- fct_churn: Churn events fact table
-- Grain: one row per churn event

with churn as (
    select * from "ravenstack"."main_staging"."stg_churn_events"
),

accounts as (
    select account_id, account_key
    from "ravenstack"."main_marts"."dim_account"
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
    );
  
  ;
-- created_at: 2026-03-14T09:27:39.663526400+00:00
-- finished_at: 2026-03-14T09:27:39.664040900+00:00
-- elapsed: 514us
-- outcome: success
-- dialect: duckdb
-- node_id: model.dbt_ravenstack.fct_churn
-- query_id: not available
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.dbt_ravenstack.fct_churn", "profile_name": "dbt_ravenstack", "target_name": "dev"} */

    SELECT index_name
    FROM duckdb_indexes()
    WHERE schema_name = 'main_marts'
      AND table_name = 'fct_churn'
  ;
-- created_at: 2026-03-14T09:27:39.664576900+00:00
-- finished_at: 2026-03-14T09:27:39.664960200+00:00
-- elapsed: 383us
-- outcome: success
-- dialect: duckdb
-- node_id: model.dbt_ravenstack.fct_churn
-- query_id: not available
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.dbt_ravenstack.fct_churn", "profile_name": "dbt_ravenstack", "target_name": "dev"} */

    SELECT COUNT(*) as remaining_indexes
    FROM duckdb_indexes()
    WHERE schema_name = 'main_marts'
      AND table_name = 'fct_churn'
  ;
-- created_at: 2026-03-14T09:27:39.665697100+00:00
-- finished_at: 2026-03-14T09:27:39.666940700+00:00
-- elapsed: 1ms
-- outcome: success
-- dialect: duckdb
-- node_id: model.dbt_ravenstack.fct_churn
-- query_id: not available
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.dbt_ravenstack.fct_churn", "profile_name": "dbt_ravenstack", "target_name": "dev"} */
alter table "ravenstack"."main_marts"."fct_churn" rename to "fct_churn__dbt_backup";
-- created_at: 2026-03-14T09:27:39.657852100+00:00
-- finished_at: 2026-03-14T09:27:39.670165100+00:00
-- elapsed: 12ms
-- outcome: success
-- dialect: duckdb
-- node_id: model.dbt_ravenstack.fct_subscriptions
-- query_id: not available
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.dbt_ravenstack.fct_subscriptions", "profile_name": "dbt_ravenstack", "target_name": "dev"} */

  
    
    

    create  table
      "ravenstack"."main_marts"."fct_subscriptions__dbt_tmp"
  
    as (
      -- fct_subscriptions: Subscription fact table
-- Grain: one row per subscription event

with subscriptions as (
    select * from "ravenstack"."main_staging"."stg_subscriptions"
),

accounts as (
    select account_id, account_key
    from "ravenstack"."main_marts"."dim_account"
),

plans as (
    select plan_key, plan_tier, billing_frequency
    from "ravenstack"."main_marts"."dim_plan"
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
    );
  
  ;
-- created_at: 2026-03-14T09:27:39.667737600+00:00
-- finished_at: 2026-03-14T09:27:39.670803800+00:00
-- elapsed: 3ms
-- outcome: success
-- dialect: duckdb
-- node_id: model.dbt_ravenstack.fct_churn
-- query_id: not available
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.dbt_ravenstack.fct_churn", "profile_name": "dbt_ravenstack", "target_name": "dev"} */
alter table "ravenstack"."main_marts"."fct_churn__dbt_tmp" rename to "fct_churn";
-- created_at: 2026-03-14T09:27:39.670905900+00:00
-- finished_at: 2026-03-14T09:27:39.671355200+00:00
-- elapsed: 449us
-- outcome: success
-- dialect: duckdb
-- node_id: model.dbt_ravenstack.fct_subscriptions
-- query_id: not available
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.dbt_ravenstack.fct_subscriptions", "profile_name": "dbt_ravenstack", "target_name": "dev"} */

    SELECT index_name
    FROM duckdb_indexes()
    WHERE schema_name = 'main_marts'
      AND table_name = 'fct_subscriptions'
  ;
-- created_at: 2026-03-14T09:27:39.671857300+00:00
-- finished_at: 2026-03-14T09:27:39.672247300+00:00
-- elapsed: 390us
-- outcome: success
-- dialect: duckdb
-- node_id: model.dbt_ravenstack.fct_subscriptions
-- query_id: not available
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.dbt_ravenstack.fct_subscriptions", "profile_name": "dbt_ravenstack", "target_name": "dev"} */

    SELECT COUNT(*) as remaining_indexes
    FROM duckdb_indexes()
    WHERE schema_name = 'main_marts'
      AND table_name = 'fct_subscriptions'
  ;
-- created_at: 2026-03-14T09:27:39.672069400+00:00
-- finished_at: 2026-03-14T09:27:39.673178400+00:00
-- elapsed: 1ms
-- outcome: success
-- dialect: duckdb
-- node_id: model.dbt_ravenstack.fct_churn
-- query_id: not available
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.dbt_ravenstack.fct_churn", "profile_name": "dbt_ravenstack", "target_name": "dev"} */

      drop table if exists "ravenstack"."main_marts"."fct_churn__dbt_backup" cascade
    ;
-- created_at: 2026-03-14T09:27:39.672985100+00:00
-- finished_at: 2026-03-14T09:27:39.673959400+00:00
-- elapsed: 974us
-- outcome: success
-- dialect: duckdb
-- node_id: model.dbt_ravenstack.fct_subscriptions
-- query_id: not available
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.dbt_ravenstack.fct_subscriptions", "profile_name": "dbt_ravenstack", "target_name": "dev"} */
alter table "ravenstack"."main_marts"."fct_subscriptions" rename to "fct_subscriptions__dbt_backup";
-- created_at: 2026-03-14T09:27:39.674734300+00:00
-- finished_at: 2026-03-14T09:27:39.675692900+00:00
-- elapsed: 958us
-- outcome: success
-- dialect: duckdb
-- node_id: model.dbt_ravenstack.fct_subscriptions
-- query_id: not available
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.dbt_ravenstack.fct_subscriptions", "profile_name": "dbt_ravenstack", "target_name": "dev"} */
alter table "ravenstack"."main_marts"."fct_subscriptions__dbt_tmp" rename to "fct_subscriptions";
-- created_at: 2026-03-14T09:27:39.675704600+00:00
-- finished_at: 2026-03-14T09:27:39.676403700+00:00
-- elapsed: 699us
-- outcome: success
-- dialect: duckdb
-- node_id: test.dbt_ravenstack.not_null_fct_churn_churn_key.6e960a2018
-- query_id: not available
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.dbt_ravenstack.not_null_fct_churn_churn_key.6e960a2018", "profile_name": "dbt_ravenstack", "target_name": "dev"} */

    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select churn_key
from "ravenstack"."main_marts"."fct_churn"
where churn_key is null



  
  
      
    ) dbt_internal_test;
-- created_at: 2026-03-14T09:27:39.677331+00:00
-- finished_at: 2026-03-14T09:27:39.678429500+00:00
-- elapsed: 1ms
-- outcome: success
-- dialect: duckdb
-- node_id: model.dbt_ravenstack.fct_subscriptions
-- query_id: not available
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.dbt_ravenstack.fct_subscriptions", "profile_name": "dbt_ravenstack", "target_name": "dev"} */

      drop table if exists "ravenstack"."main_marts"."fct_subscriptions__dbt_backup" cascade
    ;
-- created_at: 2026-03-14T09:27:39.675883100+00:00
-- finished_at: 2026-03-14T09:27:39.678564900+00:00
-- elapsed: 2ms
-- outcome: success
-- dialect: duckdb
-- node_id: test.dbt_ravenstack.unique_fct_churn_churn_key.652a1debe2
-- query_id: not available
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.dbt_ravenstack.unique_fct_churn_churn_key.652a1debe2", "profile_name": "dbt_ravenstack", "target_name": "dev"} */

    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    churn_key as unique_field,
    count(*) as n_records

from "ravenstack"."main_marts"."fct_churn"
where churn_key is not null
group by churn_key
having count(*) > 1



  
  
      
    ) dbt_internal_test;
-- created_at: 2026-03-14T09:27:39.680905800+00:00
-- finished_at: 2026-03-14T09:27:39.681389900+00:00
-- elapsed: 484us
-- outcome: success
-- dialect: duckdb
-- node_id: test.dbt_ravenstack.not_null_fct_subscriptions_subscription_key.4aa131c4de
-- query_id: not available
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.dbt_ravenstack.not_null_fct_subscriptions_subscription_key.4aa131c4de", "profile_name": "dbt_ravenstack", "target_name": "dev"} */

    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select subscription_key
from "ravenstack"."main_marts"."fct_subscriptions"
where subscription_key is null



  
  
      
    ) dbt_internal_test;
-- created_at: 2026-03-14T09:27:39.680936300+00:00
-- finished_at: 2026-03-14T09:27:39.683186900+00:00
-- elapsed: 2ms
-- outcome: success
-- dialect: duckdb
-- node_id: test.dbt_ravenstack.unique_fct_subscriptions_subscription_key.5863fdd5b8
-- query_id: not available
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.dbt_ravenstack.unique_fct_subscriptions_subscription_key.5863fdd5b8", "profile_name": "dbt_ravenstack", "target_name": "dev"} */

    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    subscription_key as unique_field,
    count(*) as n_records

from "ravenstack"."main_marts"."fct_subscriptions"
where subscription_key is not null
group by subscription_key
having count(*) > 1



  
  
      
    ) dbt_internal_test;
-- created_at: 2026-03-14T09:27:39.686078800+00:00
-- finished_at: 2026-03-14T09:27:39.715263+00:00
-- elapsed: 29ms
-- outcome: success
-- dialect: duckdb
-- node_id: model.dbt_ravenstack.fct_feature_usage
-- query_id: not available
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.dbt_ravenstack.fct_feature_usage", "profile_name": "dbt_ravenstack", "target_name": "dev"} */

  
    
    

    create  table
      "ravenstack"."main_marts"."fct_feature_usage__dbt_tmp"
  
    as (
      -- fct_feature_usage: Feature usage fact table
-- Grain: one row per daily feature usage log

with usage as (
    select * from "ravenstack"."main_staging"."stg_feature_usage"
),

subscriptions as (
    select subscription_id, subscription_key, account_key
    from "ravenstack"."main_marts"."fct_subscriptions"
)

select
    md5(cast(coalesce(cast(u.usage_id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT))  as usage_key,
    u.usage_id,

    -- Foreign keys
    s.subscription_key,
    s.account_key,

    -- Dimensions
    u.usage_date,
    u.feature_name,
    u.is_beta_feature,

    -- Measures
    u.usage_count,
    u.usage_duration_secs,
    round(u.usage_duration_secs / 60.0, 2)            as usage_duration_mins,
    u.error_count,
    case when u.error_count > 0 then true else false end as had_errors

from usage u
left join subscriptions s on u.subscription_id = s.subscription_id
    );
  
  ;
-- created_at: 2026-03-14T09:27:39.715972800+00:00
-- finished_at: 2026-03-14T09:27:39.716430200+00:00
-- elapsed: 457us
-- outcome: success
-- dialect: duckdb
-- node_id: model.dbt_ravenstack.fct_feature_usage
-- query_id: not available
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.dbt_ravenstack.fct_feature_usage", "profile_name": "dbt_ravenstack", "target_name": "dev"} */

    SELECT index_name
    FROM duckdb_indexes()
    WHERE schema_name = 'main_marts'
      AND table_name = 'fct_feature_usage'
  ;
-- created_at: 2026-03-14T09:27:39.716926700+00:00
-- finished_at: 2026-03-14T09:27:39.717406400+00:00
-- elapsed: 479us
-- outcome: success
-- dialect: duckdb
-- node_id: model.dbt_ravenstack.fct_feature_usage
-- query_id: not available
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.dbt_ravenstack.fct_feature_usage", "profile_name": "dbt_ravenstack", "target_name": "dev"} */

    SELECT COUNT(*) as remaining_indexes
    FROM duckdb_indexes()
    WHERE schema_name = 'main_marts'
      AND table_name = 'fct_feature_usage'
  ;
-- created_at: 2026-03-14T09:27:39.718166400+00:00
-- finished_at: 2026-03-14T09:27:39.719076100+00:00
-- elapsed: 909us
-- outcome: success
-- dialect: duckdb
-- node_id: model.dbt_ravenstack.fct_feature_usage
-- query_id: not available
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.dbt_ravenstack.fct_feature_usage", "profile_name": "dbt_ravenstack", "target_name": "dev"} */
alter table "ravenstack"."main_marts"."fct_feature_usage" rename to "fct_feature_usage__dbt_backup";
-- created_at: 2026-03-14T09:27:39.719755500+00:00
-- finished_at: 2026-03-14T09:27:39.720548600+00:00
-- elapsed: 793us
-- outcome: success
-- dialect: duckdb
-- node_id: model.dbt_ravenstack.fct_feature_usage
-- query_id: not available
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.dbt_ravenstack.fct_feature_usage", "profile_name": "dbt_ravenstack", "target_name": "dev"} */
alter table "ravenstack"."main_marts"."fct_feature_usage__dbt_tmp" rename to "fct_feature_usage";
-- created_at: 2026-03-14T09:27:39.721679200+00:00
-- finished_at: 2026-03-14T09:27:39.722556500+00:00
-- elapsed: 877us
-- outcome: success
-- dialect: duckdb
-- node_id: model.dbt_ravenstack.fct_feature_usage
-- query_id: not available
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.dbt_ravenstack.fct_feature_usage", "profile_name": "dbt_ravenstack", "target_name": "dev"} */

      drop table if exists "ravenstack"."main_marts"."fct_feature_usage__dbt_backup" cascade
    ;
-- created_at: 2026-03-14T09:27:39.725573600+00:00
-- finished_at: 2026-03-14T09:27:39.726071200+00:00
-- elapsed: 497us
-- outcome: success
-- dialect: duckdb
-- node_id: test.dbt_ravenstack.not_null_fct_feature_usage_usage_key.60917a2602
-- query_id: not available
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.dbt_ravenstack.not_null_fct_feature_usage_usage_key.60917a2602", "profile_name": "dbt_ravenstack", "target_name": "dev"} */

    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select usage_key
from "ravenstack"."main_marts"."fct_feature_usage"
where usage_key is null



  
  
      
    ) dbt_internal_test;
-- created_at: 2026-03-14T09:27:39.725451800+00:00
-- finished_at: 2026-03-14T09:27:39.728863900+00:00
-- elapsed: 3ms
-- outcome: success
-- dialect: duckdb
-- node_id: test.dbt_ravenstack.unique_fct_feature_usage_usage_key.e039614975
-- query_id: not available
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.dbt_ravenstack.unique_fct_feature_usage_usage_key.e039614975", "profile_name": "dbt_ravenstack", "target_name": "dev"} */

    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    usage_key as unique_field,
    count(*) as n_records

from "ravenstack"."main_marts"."fct_feature_usage"
where usage_key is not null
group by usage_key
having count(*) > 1



  
  
      
    ) dbt_internal_test;
