
    
    

select
    usage_key as unique_field,
    count(*) as n_records

from "ravenstack"."main_marts"."fct_feature_usage"
where usage_key is not null
group by usage_key
having count(*) > 1


