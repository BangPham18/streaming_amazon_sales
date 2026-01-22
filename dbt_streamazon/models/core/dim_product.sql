{{ config(materialized = 'table') }}

-- Dim_Product: Bảng dimension chứa thông tin sản phẩm
-- Mục đích: Phân tích doanh số theo loại sản phẩm, kích cỡ, kiểu dáng


SELECT 
    {{ dbt_utils.generate_surrogate_key(['sku', 'asin', 'style', 'category', 'size']) }} as product_key,
    *
FROM (
    SELECT DISTINCT
        sku,
        asin,
        style,
        category,
        size
    FROM {{ source('staging', 'amazon_sales_external') }}
    WHERE sku IS NOT NULL OR asin IS NOT NULL

    UNION ALL

-- Default row for unknown products
SELECT 
        'NA' as sku,
        'NA' as asin,
        'NA' as style,
        'NA' as category,
        'NA' as size
)