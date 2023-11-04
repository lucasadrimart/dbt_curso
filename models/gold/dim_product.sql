with stg_product as
    (
        select *
        from {{ref('stg_product')}}
    ),
stg_productcategory as
    (
        select *
        from {{ref('stg_productcategory')}}
    
    ),
stg_productsubcategory as
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
        stg_productcategory.product_category_name,
        stg_productsubcategory.product_subcategory_name
    from
        stg_product
        left join stg_productsubcategory
        on stg_product.productsubcategoryid = stg_productsubcategory.productsubcategoryid
        left join stg_productcategory
        on stg_productsubcategory.productcategoryid = stg_productcategory.productcategoryid

)

select * from transformation