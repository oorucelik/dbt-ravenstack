 -- noqa: Should accept a string instead of a integer
    
    
    truncate table "ravenstack"."main_raw"."ravenstack_subscriptions";
    -- dbt seed --
    
          COPY "ravenstack"."main_raw"."ravenstack_subscriptions" FROM 'C:\BlogIdeas\dbt_ravenstack\seeds\ravenstack_subscriptions.csv' (FORMAT CSV, HEADER TRUE, DELIMITER ',')
        

;
  