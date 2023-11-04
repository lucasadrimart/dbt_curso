with stg_product as
    (
        select *
        from {{ref('stg_product')}}
    ),
stg_product_category as
    (
        select *
        from {{ref('stg_productcategory')}}
    
    ),
stg_product_subcategory as
    (
        select *
        from {{ref('stg_productsubcategory')}}
    
    ),


transformation as


(
    select
        {{ dbt_utils.generate_surrogate_key(['stg_product.productid'])}} as product_key,
        stg_product.productid,
        stg_product.product_name,
        stg_product.productnumber,
        stg_product.color,
        stg_product.class,
        stg_product_category.product_category_name,
        stg_product_subcategory.product_subcategory_name
    from
        stg_product
        left join stg_product_subcategory PSC
        on stg_product.productsubcategoryid = PSC.productsubcategoryid
        left join stg_product_category PC
        on PSC.productcategoryid = PC.productcategoryid

)

select * from transformation