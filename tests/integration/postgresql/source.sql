-- 明示マップの列名、原本の小数精度、UTC・NULLの扱いを同時に確認する。
CREATE SCHEMA factory_erp;
CREATE SCHEMA factory_mes;

CREATE TABLE factory_erp.products (
  product_id text PRIMARY KEY, product_name text, product_family varchar(80),
  unit text, standard_cycle_seconds numeric(12,4), updated_at timestamptz NOT NULL
);
CREATE TABLE factory_erp.work_orders (
  work_order_id text PRIMARY KEY, factory_id text, product_id text,
  planned_date date, planned_qty integer, updated_at timestamptz NOT NULL
);
CREATE TABLE factory_erp.sales_orders (
  sales_order_id text PRIMARY KEY, factory_id text, product_id text,
  placed_day date, promised_date date, ordered_qty integer,
  unit_price numeric(14,4), updated_at timestamptz NOT NULL
);
CREATE TABLE factory_erp.shipments (
  shipment_id text PRIMARY KEY, sales_order_id text, shipped_at timestamptz,
  shipped_qty integer, updated_at timestamptz NOT NULL
);
CREATE TABLE factory_mes.events (
  event_id text PRIMARY KEY, work_order_id text, event_time timestamptz,
  good_qty integer, reject_qty integer, updated_at timestamptz NOT NULL
);
CREATE TABLE factory_mes.downtime (
  downtime_id text PRIMARY KEY, work_order_id text, start_time timestamptz,
  end_time timestamptz, reason_code text, updated_at timestamptz NOT NULL
);

INSERT INTO factory_erp.products VALUES
  ('P01','軸受','標準','EA',12.3400,'2026-01-05 12:00:00.123456+09');
INSERT INTO factory_erp.work_orders VALUES
  ('WO1','F1','P01','2026-01-05',20,'2026-01-05 12:00:00.123456+09');
INSERT INTO factory_erp.sales_orders VALUES
  ('SO1','F1','P01','2026-01-05','2026-01-07',10,2.3000,'2026-01-05 12:00:00.123456+09');
INSERT INTO factory_erp.shipments VALUES
  ('SH1','SO1','2026-01-06 08:00:00.234567+09',3,'2026-01-05 12:00:00.123456+09');
INSERT INTO factory_mes.events VALUES
  ('E1','WO1','2026-01-05 08:00:00.876543Z',8,1,'2026-01-05 12:00:00.123456+09');
INSERT INTO factory_mes.downtime VALUES
  ('DT1','WO1','2026-01-05 08:00:00.012345Z','2026-01-05 08:01:30.012345Z',NULL,'2026-01-05 12:00:00.123456+09');
