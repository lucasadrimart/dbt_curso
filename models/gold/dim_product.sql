with stg_product as
    (
        select *
        from {{ref('stg.product')}}
    ),
stg_product_category as
    (
        select *
        from {{ref('stg.productcategory')}}
    
    ),
stg_product_subcategory as
    (
        select *
        from {{ref('stg.productsubcategory')}}
    
    ),


transformation as


(
    select 
        {{ dbt.utils.generate_surrogate_key(['stg_product_productid'])}} as product_key,
        stg_product.productid,
        stg_product.product_name,
        stg_product.productnumber,
        stg_product.color,
        stg_product.class,
        stg_product_category.product_category_name
        stg_product_subcategory.product_subcategory_name
    from
        stg_product
        left join stg_product_subcategory PSC
        on stg_product_productid = PSC.productsubcategoryid
        left join stg_product_category PC
        on PSC.productsubcategoryid = PC.productcategoryid

)

select * from transformation