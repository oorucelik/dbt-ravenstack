 -- noqa: Should accept a string instead of a integer
    
    
    truncate table "ravenstack"."main_raw"."ravenstack_feature_usage";
    -- dbt seed --
    
          COPY "ravenstack"."main_raw"."ravenstack_feature_usage" FROM 'C:\BlogIdeas\dbt_ravenstack\seeds\ravenstack_feature_usage.csv' (FORMAT CSV, HEADER TRUE, DELIMITER ',')
        

;
  