-- 遅延到着、再送、取消を同じ入力で照合する小規模な試験。
WITH events (product_id, revision, operation, cycle) AS (
    VALUES
        ('P1', 10, 'c', '12.50'),
        ('P1', 20, 'u', '14.25'),
        ('P1', 20, 'u', '14.25'),
        ('P1', 15, 'u', '13.00'),
        ('P2', 10, 'c', '20.00'),
        ('P2', 30, 'd', NULL),
        ('P2', 40, 't', NULL),
        ('P3', 10, 'c', 'invalid')
),
ranked AS (
    SELECT
        product_id,
        operation,
        TRY_CAST(cycle AS DECIMAL(10, 2)) AS cycle,
        ROW_NUMBER() OVER (
            PARTITION BY product_id ORDER BY revision DESC
        ) AS position
    FROM (
        SELECT DISTINCT product_id, revision, operation, cycle
        FROM events
        WHERE operation <> 't'
    ) AS unique_events
)
SELECT
    product_id,
    CASE WHEN cycle IS NULL THEN 'invalid_type' ELSE 'accepted' END AS result,
    COALESCE(CAST(cycle AS VARCHAR), '-') AS cycle
FROM ranked
WHERE position = 1 AND operation <> 'd'
ORDER BY product_id
