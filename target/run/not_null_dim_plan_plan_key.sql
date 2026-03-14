
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select plan_key
from "ravenstack"."main_marts"."dim_plan"
where plan_key is null



  
  
      
    ) dbt_internal_test