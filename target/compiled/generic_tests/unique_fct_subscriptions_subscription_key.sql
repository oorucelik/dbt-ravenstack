
    
    

select
    subscription_key as unique_field,
    count(*) as n_records

from "ravenstack"."main_marts"."fct_subscriptions"
where subscription_key is not null
group by subscription_key
having count(*) > 1


