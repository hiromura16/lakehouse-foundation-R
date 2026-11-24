-- 日別合成Goldの受入境界。通知や書き戻しから独立した分析用の一時表。
-- APIと同じ工場×日付を主キーにし、重複と数量の不整合をロード時に検出する。
CREATE TABLE production (
    factory_id VARCHAR NOT NULL CHECK (length(factory_id) BETWEEN 1 AND 128),
    production_date DATE NOT NULL,
    planned_qty DECIMAL(38, 0) NOT NULL CHECK (planned_qty >= 0),
    good_qty DECIMAL(38, 0) NOT NULL CHECK (good_qty >= 0),
    reject_qty DECIMAL(38, 0) NOT NULL CHECK (reject_qty >= 0),
    total_qty DECIMAL(38, 0) NOT NULL CHECK (total_qty = good_qty + reject_qty),
    downtime_minutes DOUBLE NOT NULL
        CHECK (isfinite(downtime_minutes) AND downtime_minutes >= 0),
    yield_pct DOUBLE CHECK (yield_pct BETWEEN 0 AND 100),
    plan_adherence_pct DOUBLE
        CHECK (isfinite(plan_adherence_pct) AND plan_adherence_pct >= 0),
    PRIMARY KEY (factory_id, production_date)
);
