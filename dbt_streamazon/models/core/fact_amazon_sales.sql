{{ config(
    materialized = 'table',
    partition_by={
        "field": "order_date",
        "data_type": "date",
        "granularity": "day"
    }
) }}

-- Fact_Amazon_Sales: Bảng fact chứa các sự kiện bán hàng
-- Chứa các foreign keys trỏ đến các bảng dimension và các metrics


SELECT 
    sales.`Order ID` as order_id,
    PARSE_DATE('%y/%m/%d', sales.Date) as order_date,
    dim_product.product_key as product_key,
    dim_location.location_key as location_key,
    dim_order_details.order_details_key as order_details_key,
    dim_promotion.promotion_key as promotion_key,
    CAST(sales.Qty AS INT64) as qty,
    CAST(sales.Amount AS FLOAT64) as amount,
    sales.currency as currency,
    CAST(sales.B2B AS BOOLEAN) as b2b
FROM {{ source('staging', 'amazon_sales_external') }} as sales

LEFT JOIN {{ ref('dim_product') }} as dim_product
    ON sales.SKU = dim_product.sku 
    AND sales.ASIN = dim_product.asin

LEFT JOIN {{ ref('dim_location_amazon') }} as dim_location
    ON CAST(sales.`ship-postal-code` AS STRING) = dim_location.ship_postal_code
    AND sales.`ship-city` = dim_location.ship_city
    AND sales.`ship-state` = dim_location.ship_state
    AND sales.`ship-country` = dim_location.ship_country

LEFT JOIN {{ ref('dim_order_details') }} as dim_order_details
    ON sales.Fulfilment = dim_order_details.fulfilment
    AND sales.`ship-service-level` = dim_order_details.ship_service_level
    AND sales.`Courier Status` = dim_order_details.courier_status
    AND sales.Status = dim_order_details.status

LEFT JOIN {{ ref('dim_promotion') }} as dim_promotion
    ON COALESCE(sales.`promotion-ids`, 'No Promotion') = dim_promotion.promotion_ids