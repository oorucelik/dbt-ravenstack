 -- noqa: Should accept a string instead of a integer
    
    
    truncate table "ravenstack"."main_raw"."ravenstack_accounts";
    -- dbt seed --
    
          COPY "ravenstack"."main_raw"."ravenstack_accounts" FROM 'C:\BlogIdeas\dbt_ravenstack\seeds\ravenstack_accounts.csv' (FORMAT CSV, HEADER TRUE, DELIMITER ',')
        

;
  