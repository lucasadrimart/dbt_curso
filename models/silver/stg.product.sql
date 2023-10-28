with source_data as (
    select
        productid
        , name as product_name
        , safetystocklevel
         



    from {{source('adventureworks-gcp','product')}}
)

select * 
from source_data