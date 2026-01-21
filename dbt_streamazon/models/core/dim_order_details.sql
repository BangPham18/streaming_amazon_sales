{{ config(materialized = 'table') }}

-- Dim_OrderDetails: Bảng dimension chứa thông tin xử lý đơn hàng
-- Mục đích: Phân tích hiệu quả giao hàng, tỷ lệ hủy đơn theo phương thức vận chuyển


SELECT 
    {{ dbt_utils.generate_surrogate_key(['fulfilment', 'ship_service_level', 'courier_status', 'status']) }} as order_details_key,
    *
FROM (
    SELECT DISTINCT
        Fulfilment as fulfilment,
        `ship-service-level` as ship_service_level,
        `Courier Status` as courier_status,
        Status as status
    FROM {{ source('staging', 'amazon_sales_external') }}

    UNION ALL

-- Default row for unknown order details
SELECT 
        'NA' as fulfilment,
        'NA' as ship_service_level,
        'NA' as courier_status,
        'NA' as status
)