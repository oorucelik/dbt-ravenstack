
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select ticket_id
from "ravenstack"."raw"."ravenstack_support_tickets"
where ticket_id is null



  
  
      
    ) dbt_internal_test