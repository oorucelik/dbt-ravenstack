
    
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



  
  
      
    ) dbt_internal_test