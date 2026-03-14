
    
    

select
    churn_key as unique_field,
    count(*) as n_records

from "ravenstack"."main_marts"."fct_churn"
where churn_key is not null
group by churn_key
having count(*) > 1


