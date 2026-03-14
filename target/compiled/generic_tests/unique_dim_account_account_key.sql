
    
    

select
    account_key as unique_field,
    count(*) as n_records

from "ravenstack"."main_marts"."dim_account"
where account_key is not null
group by account_key
having count(*) > 1


