 -- noqa: Should accept a string instead of a integer
    
    
    truncate table "ravenstack"."main_raw"."ravenstack_support_tickets";
    -- dbt seed --
    
          COPY "ravenstack"."main_raw"."ravenstack_support_tickets" FROM 'C:\BlogIdeas\dbt_ravenstack\seeds\ravenstack_support_tickets.csv' (FORMAT CSV, HEADER TRUE, DELIMITER ',')
        

;
  