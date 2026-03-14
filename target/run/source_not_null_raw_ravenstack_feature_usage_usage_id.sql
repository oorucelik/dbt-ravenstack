
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select usage_id
from "ravenstack"."raw"."ravenstack_feature_usage"
where usage_id is null



  
  
      
    ) dbt_internal_test