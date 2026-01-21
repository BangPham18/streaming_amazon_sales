# Schema definitions for Amazon Sales data

schema = {
    "amazon_sales": [
        {"name": "index", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "order_id", "type": "STRING", "mode": "NULLABLE"},
        {"name": "date", "type": "STRING", "mode": "NULLABLE"},
        {"name": "status", "type": "STRING", "mode": "NULLABLE"},
        {"name": "fulfilment", "type": "STRING", "mode": "NULLABLE"},
        {"name": "sales_channel", "type": "STRING", "mode": "NULLABLE"},
        {"name": "ship_service_level", "type": "STRING", "mode": "NULLABLE"},
        {"name": "style", "type": "STRING", "mode": "NULLABLE"},
        {"name": "sku", "type": "STRING", "mode": "NULLABLE"},
        {"name": "category", "type": "STRING", "mode": "NULLABLE"},
        {"name": "size", "type": "STRING", "mode": "NULLABLE"},
        {"name": "asin", "type": "STRING", "mode": "NULLABLE"},
        {"name": "courier_status", "type": "STRING", "mode": "NULLABLE"},
        {"name": "qty", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "currency", "type": "STRING", "mode": "NULLABLE"},
        {"name": "amount", "type": "FLOAT64", "mode": "NULLABLE"},
        {"name": "ship_city", "type": "STRING", "mode": "NULLABLE"},
        {"name": "ship_state", "type": "STRING", "mode": "NULLABLE"},
        {"name": "ship_postal_code", "type": "STRING", "mode": "NULLABLE"},
        {"name": "ship_country", "type": "STRING", "mode": "NULLABLE"},
        {"name": "promotion_ids", "type": "STRING", "mode": "NULLABLE"},
        {"name": "b2b", "type": "BOOLEAN", "mode": "NULLABLE"},
        {"name": "fulfilled_by", "type": "STRING", "mode": "NULLABLE"},
    ]
}