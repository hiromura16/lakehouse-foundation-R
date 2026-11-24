"""Run the Foundation business SQL with DuckDB and publish Iceberg snapshots.

The catalog and object store retain table state. DuckDB starts in memory for
each run, with no network extensions, local input files or user-supplied SQL.
Only the batch runner resolves table names and translates checked-in SQL.
"""

import json
import re
from datetime import date, datetime, timezone
from decimal import Decimal
from hashlib import sha256
from pathlib import Path

import duckdb
import pyarrow as pa

from native import sensor

ROOT = Path(__file__).resolve().parents[1]
SQL_ROOT = ROOT / "medallion"
MAX_ROWS = 100_000
MAX_TABLE_BYTES = 64 * 1024 * 1024
LAYERS = ("bronze", "silver", "quarantine", "gold")
GOLD = {
    "factory_daily_production": ("factory_id", "production_date"),
    "sales_fulfillment": ("sales_order_id",),
    "sensor_latest": (
        "factory_id",
        "line_id",
        "equipment_id",
        "sensor_id",
        "measurement",
        "unit",
    ),
    "sensor_minute": (
        "factory_id",
        "line_id",
        "equipment_id",
        "sensor_id",
        "measurement",
        "unit",
        "minute",
    ),
}
RELEASE_SCHEMA = pa.schema(
    [("released_at", pa.string()), ("manifest_json", pa.string())]
)
CTAS = re.compile(
    r"^CREATE OR REPLACE TABLE ((?:silver|quarantine|gold)\.\w+) AS\s+",
    re.IGNORECASE,
)


def connect():
    """Use a fresh UTC connection restricted to explicitly registered Arrow."""
    db = duckdb.connect(
        ":memory:",
        config={
            "threads": "2",
            "memory_limit": "640MB",
            "enable_external_access": "false",
            "autoinstall_known_extensions": "false",
            "autoload_known_extensions": "false",
        },
    )
    db.execute("SET TimeZone = 'UTC'")
    return db


def sql_for_duckdb(sql):
    """Adapt the audited Foundation dialect constructs without changing rules.

    The Trino source files are the business contract. The substitutions here
    are for those files and the independent acceptance SQL only. A change to
    the source hash and the oracle tests must accompany new SQL constructs.
    """
    sql = re.sub(r"^\s*--.*$", "", sql, flags=re.MULTILINE)
    sql = re.sub(
        r"\biceberg\.(?=bronze\.|silver\.|quarantine\.|gold\.)", "", sql
    )
    sql = re.sub(
        r"TRY\(FROM_ISO8601_TIMESTAMP\((\w+)\)\)",
        r"TRY_CAST(\1 AS TIMESTAMPTZ)",
        sql,
        flags=re.IGNORECASE,
    )
    sql = re.sub(
        r"JSON_FORMAT\(CAST\(MAP\((.*?)\) AS JSON\)\)",
        r"CAST(TO_JSON(MAP(\1)) AS VARCHAR)",
        sql,
        flags=re.DOTALL | re.IGNORECASE,
    )
    # DuckDB BIGINT rounds a decimal string while Trino's VARCHAR cast rejects
    # it. Validation must preserve the original invalid_type outcome.
    sql = re.sub(
        r"TRY_CAST\((\w+) AS BIGINT\)",
        r"CASE WHEN regexp_full_match(trim(\1), '[+-]?[0-9]+') "
        r"THEN TRY_CAST(\1 AS BIGINT) END",
        sql,
        flags=re.IGNORECASE,
    )
    sql = re.sub(
        r"date_diff\('second', (\w+\.\w+), (\w+\.\w+)\)",
        r"CAST(trunc(epoch(\2 - \1)) AS BIGINT)",
        sql,
        flags=re.IGNORECASE,
    )
    return sql.replace("json_extract_scalar(", "json_extract_string(").strip()


def bounded_scan(table, snapshot_id):
    """Read one pinned snapshot and reject oversized batches as they arrive."""
    if snapshot_id is None:
        return table.schema().as_arrow().empty_table()
    batches = []
    row_count = byte_count = 0
    reader = table.scan(
        snapshot_id=snapshot_id, limit=MAX_ROWS + 1
    ).to_arrow_batch_reader()
    for batch in reader:
        row_count += batch.num_rows
        byte_count += batch.nbytes
        if row_count > MAX_ROWS or byte_count > MAX_TABLE_BYTES:
            raise ValueError("NATIVE_BATCH_LIMIT")
        batches.append(batch)
    return pa.Table.from_batches(batches, schema=reader.schema)


def current_id(table):
    """Return the exact main snapshot, or None for a newly empty table."""
    snapshot = table.current_snapshot()
    return snapshot.snapshot_id if snapshot else None


def write_table(catalog, identifier, arrow):
    """Overwrite one Iceberg V2 table with stable schema and return its ID."""
    if not catalog.table_exists(identifier):
        catalog.create_table(
            identifier, schema=arrow.schema, properties={"format-version": "2"}
        )
    table = catalog.load_table(identifier)
    expected = table.schema().as_arrow()
    if not arrow.schema.equals(expected, check_metadata=False):
        # DuckDB can canonicalize Arrow timezone aliases and string widths.
        # Arrow rejects incompatible casts before the catalog commit.
        arrow = arrow.cast(expected)
    if arrow.num_rows or current_id(table) is not None:
        table.overwrite(arrow)
    return current_id(table)


def release_table(core, prefix):
    """Resolve one manifest table inside the matching batch/CDC namespace."""
    namespace = core.schema(prefix, "gold")
    return f"{namespace}._release"


def latest_release(core, catalog, prefix="", release_id=None):
    """Load a pinned or latest successful manifest, including its own ID."""
    identifier = release_table(core, prefix)
    if not catalog.table_exists(identifier):
        return None
    table = catalog.load_table(identifier)
    snapshot_id = current_id(table) if release_id is None else release_id
    if snapshot_id is None or table.snapshot_by_id(snapshot_id) is None:
        return None
    (row,) = table.scan(snapshot_id=snapshot_id).to_arrow().to_pylist()
    return {
        "release_snapshot_id": str(snapshot_id),
        "released_at": row["released_at"],
        **json.loads(row["manifest_json"]),
    }


def input_tables(core, catalog, prefix):
    """Pin the six Bronze tables before loading any rows from storage."""
    identifiers = {
        source["table"]: catalog.load_table(
            f"{core.schema(prefix, 'bronze')}.{source['table']}"
        )
        for source in core.CONTRACT
    }
    return {
        name: (table, current_id(table)) for name, table in identifiers.items()
    }


def add_arrow(db, relation, target, arrow):
    """Materialize an Arrow input and release its Python reference."""
    db.register(relation, arrow)
    try:
        db.execute(f"CREATE TABLE {target} AS SELECT * FROM {relation}")
    finally:
        db.unregister(relation)


def medallion(core, prefix=""):
    """Commit derived tables and publish their snapshot IDs as the last step.

    Iceberg commits individual tables. Readers use the previous manifest
    until every new output commit succeeds. Restart and rerun after failure.
    The single-writer rule applies per prefix and must hold across processes.
    """
    core.ensure_tables(prefix)
    catalog = core.catalog()
    tables = input_tables(core, catalog, prefix)
    inputs = {name: snap for name, (_, snap) in tables.items()}
    sensor_entries = sensor.ledger(core, catalog, prefix)
    sensor_inputs = {row["path"]: row["sha256"] for row in sensor_entries}
    recipe_hash = sha256(Path(__file__).read_bytes())
    recipe_hash.update(Path(sensor.__file__).read_bytes())
    statements = []
    for path in sorted(SQL_ROOT.glob("*.sql")):
        source = path.read_bytes()
        recipe_hash.update(source)
        query = source.decode()
        if prefix in {"sql_", "sqlcdc_"}:
            query = core.with_sql_provenance(query)
        statements.extend(
            part.strip()
            for part in sql_for_duckdb(query).split(";")
            if part.strip()
        )
    recipe = recipe_hash.hexdigest()
    previous = latest_release(core, catalog, prefix)
    if (
        previous
        and previous["inputs"] == inputs
        and previous.get("sensor_inputs", {}) == sensor_inputs
        and previous["recipe"] == recipe
    ):
        return {"sql_statements": 0, "unchanged": True}
    outputs = {}
    with connect() as db:
        for layer in LAYERS:
            db.execute(f"CREATE SCHEMA {layer}")
        for name, (table, snap) in tables.items():
            add_arrow(
                db, "input_arrow", f"bronze.{name}", bounded_scan(table, snap)
            )
        for statement in statements:
            match = CTAS.match(statement)
            if not match:
                raise ValueError("NATIVE_SQL_CONTRACT")
            local = match[1]
            db.execute(statement)
            arrow = db.sql(f"SELECT * FROM {local}").to_arrow_table()
            # Trino SUM(BIGINT) produces BIGINT. DuckDB's integer SUM produces
            # HUGEINT; explicitly narrow counts and retain true decimals.
            columns = [
                pa.field(field.name, pa.int64())
                if pa.types.is_decimal(field.type) and field.type.scale == 0
                else field
                for field in arrow.schema
            ]
            arrow = arrow.cast(pa.schema(columns))
            layer, name = local.split(".")
            identifier = f"{core.schema(prefix, layer)}.{name}"
            outputs[local] = write_table(catalog, identifier, arrow)
        if not prefix:
            for local, arrow in sensor.tables(
                core, db, sensor_entries
            ).items():
                outputs[local] = write_table(catalog, local, arrow)
    release = pa.Table.from_pylist(
        [
            {
                "released_at": datetime.now(timezone.utc).isoformat(),
                "manifest_json": json.dumps(
                    {
                        "inputs": inputs,
                        "outputs": outputs,
                        "recipe": recipe,
                        "sensor_inputs": sensor_inputs,
                    },
                    sort_keys=True,
                ),
            }
        ],
        schema=RELEASE_SCHEMA,
    )
    write_table(catalog, release_table(core, prefix), release)
    return {"sql_statements": len(statements), "unchanged": False}


def verify_query(core, sql, prefix):
    """Check published Silver, Quarantine, Gold and pinned Bronze in DuckDB."""
    catalog = core.catalog()
    release = latest_release(core, catalog, prefix)
    with connect() as db:
        for layer in LAYERS:
            db.execute(f"CREATE SCHEMA {layer}")
        for name, snapshot_id in release["inputs"].items():
            table = catalog.load_table(
                f"{core.schema(prefix, 'bronze')}.{name}"
            )
            add_arrow(
                db,
                "verify_arrow",
                f"bronze.{name}",
                bounded_scan(table, snapshot_id),
            )
        for name, snapshot_id in release["outputs"].items():
            layer, table_name = name.split(".")
            table = catalog.load_table(
                f"{core.schema(prefix, layer)}.{table_name}"
            )
            add_arrow(
                db, "verify_arrow", name, bounded_scan(table, snapshot_id)
            )
        rows = db.execute(sql_for_duckdb(sql)).fetchall()

    # Trino REST encodes dates and decimals into JSON scalar values.
    def oracle_value(value):
        """Match the independent reference JSON's scalar representation."""
        if isinstance(value, date):
            return value.isoformat()
        return float(value) if isinstance(value, Decimal) else value

    return [[oracle_value(value) for value in row] for row in rows]


def gold_rows(core, name, limit, prefix="", release_id=None):
    """Return bounded, deterministically ordered Gold rows with provenance."""
    if name not in GOLD or not 1 <= limit <= 200:
        raise ValueError("INVALID_GOLD_REQUEST")
    catalog = core.catalog()
    release = latest_release(core, catalog, prefix, release_id)
    local = f"gold.{name}"
    if not release or local not in release["outputs"]:
        return None
    snapshot_id = release["outputs"][local]
    identifier = f"{core.schema(prefix, 'gold')}.{name}"
    table = catalog.load_table(identifier)
    arrow = bounded_scan(table, snapshot_id)
    with connect() as db:
        db.register("gold_data", arrow)
        sort = ", ".join(GOLD[name])
        records = (
            db.sql(
                f"SELECT * FROM gold_data ORDER BY {sort} LIMIT ?",
                params=[limit],
            )
            .to_arrow_table()
            .to_pylist()
        )
    return {
        "table": identifier,
        "snapshot_id": str(snapshot_id) if snapshot_id is not None else None,
        "release_snapshot_id": release["release_snapshot_id"],
        "released_at": release["released_at"],
        "recipe": release["recipe"],
        "rows": records,
        "total_rows": arrow.num_rows,
        "truncated": arrow.num_rows > limit,
    }
