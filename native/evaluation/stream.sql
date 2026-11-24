-- 専用トピックから取得し、購読位置も検証環境に閉じ込める。
CREATE TABLE IF NOT EXISTS evaluation_products (
    product_id VARCHAR PRIMARY KEY,
    revision INT,
    cycle DOUBLE PRECISION
)
WITH (
    connector = 'kafka',
    topic = 'foundation-r-evaluation-products',
    properties.bootstrap.server = 'broker:9092',
    scan.startup.mode = 'earliest'
)
FORMAT DEBEZIUM ENCODE JSON;

CREATE MATERIALIZED VIEW IF NOT EXISTS evaluation_current AS
SELECT product_id, revision, cycle FROM evaluation_products;
