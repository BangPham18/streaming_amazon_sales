{{ config(materialized = 'table') }}

-- Dim_Location: Bảng dimension chứa thông tin địa lý giao hàng
-- Mục đích: Phân tích doanh số theo vùng miền, thành phố


SELECT 
    {{ dbt_utils.generate_surrogate_key(['ship_postal_code', 'ship_city', 'ship_state', 'ship_country']) }} as location_key,
    *
FROM (
    SELECT DISTINCT
        CAST(ship_postal_code AS STRING) as ship_postal_code,
        ship_city,
        ship_state,
        ship_country
    FROM {{ source('staging', 'amazon_sales_external') }}
    WHERE ship_city IS NOT NULL

    UNION ALL

-- Default row for unknown location
SELECT 
        'NA' as ship_postal_code,
        'NA' as ship_city,
        'NA' as ship_state,
        'NA' as ship_country
)