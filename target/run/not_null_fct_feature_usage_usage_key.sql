
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select usage_key
from "ravenstack"."main_marts"."fct_feature_usage"
where usage_key is null



  
  
      
    ) dbt_internal_test