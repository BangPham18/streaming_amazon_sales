{{ config(materialized = 'table') }}

-- Dim_Product: Bảng dimension chứa thông tin sản phẩm
-- Mục đích: Phân tích doanh số theo loại sản phẩm, kích cỡ, kiểu dáng


SELECT 
    {{ dbt_utils.generate_surrogate_key(['SKU', 'ASIN']) }} as product_key,
    *
FROM (
    SELECT DISTINCT
        SKU as sku,
        ASIN as asin,
        Style as style,
        Category as category,
        Size as size
    FROM {{ source('staging', 'amazon_sales_external') }}
    WHERE SKU IS NOT NULL OR ASIN IS NOT NULL

    UNION ALL

-- Default row for unknown products
SELECT 
        'NA' as sku,
        'NA' as asin,
        'NA' as style,
        'NA' as category,
        'NA' as size
)