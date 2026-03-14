
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select churn_event_id
from "ravenstack"."raw"."ravenstack_churn_events"
where churn_event_id is null



  
  
      
    ) dbt_internal_test