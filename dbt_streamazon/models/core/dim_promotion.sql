{{ config(materialized = 'table') }}

-- Dim_Promotion: Bảng dimension chứa thông tin khuyến mãi
-- Mục đích: Theo dõi hiệu quả của các mã khuyến mãi


SELECT 
    {{ dbt_utils.generate_surrogate_key(['promotion_ids']) }} as promotion_key,
    *
FROM (
    SELECT DISTINCT
        COALESCE(promotion_ids, 'No Promotion') as promotion_ids
    FROM {{ source('staging', 'amazon_sales_external') }}

    UNION ALL

-- Default row for no promotion
SELECT 'NA' as promotion_ids )