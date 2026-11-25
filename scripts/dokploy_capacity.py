#!/usr/bin/env python3
"""合成Composeの資源を列挙し、暫定予算との余剰・不足を照合する。"""

import json
import re
import sys
from decimal import ROUND_CEILING, Decimal, DecimalException, localcontext

import yaml
from yaml.nodes import MappingNode, ScalarNode, SequenceNode

if __package__:
    from . import dokploy_plan as plan
else:
    import dokploy_plan as plan

MIB = 1024 * 1024
DIMS = ("cpu_milli", "memory_mib")
RESERVES = ("os", "docker", "dokploy", "traefik", "existing_poc", "headroom")
MODES = {
    "core-local": "renderable",
    "local-operations": "renderable",
    "standard": "renderable",
    "zero-trust-access": "planned",
    "data-egress": "planned",
    "evaluation": "renderable",
    "all-lab": "inventory-only",
}
CATALOG = "catalog"
LEGACY = "legacy-gold-read"
ENV = re.compile(
    r"\$\$|\$\{([A-Za-z_][A-Za-z0-9_]*)(:?[?+\-])?|"
    r"\$([A-Za-z_][A-Za-z0-9_]*)"
)
ENV_KIND = {
    ":?": "required_nonempty",
    "?": "required_present",
    ":-": "has_default",
    "-": "has_default",
    ":+": "conditional",
    "+": "conditional",
    None: "no_default",
}
PAIR = re.compile(r"[0-9]{1,9}:[0-9]{1,9}", re.ASCII)


def lab_choices():
    """比較単位と前提だけを返す。一括起動の手順は提供しない。"""
    rows = [
        dict(family="core", mode="core-local"),
        dict(family="local-gui", mode="local-operations"),
        dict(family="tools", mode="standard"),
        dict(family="tools-metrics", mode="standard", tools_metrics=True),
        dict(family="cdc", mode="core-local", cdc=True),
        dict(family="alerting", mode="core-local", alerting=True),
        dict(family="legacy-gold-read", mode=LEGACY),
        *[
            dict(
                family="evaluation",
                mode="evaluation",
                profile=profile,
                **(
                    {"requires": ["distributed-runtime"]}
                    if profile == "distributed-check"
                    else {}
                ),
            )
            for profile in plan.EVALUATION
        ],
        dict(
            family="equipment-telemetry",
            status="inventory-only",
            files=["compose.evaluation.yaml", "compose.telemetry.yaml"],
            requires=["stream", "timeseries", "telemetry"],
        ),
        dict(
            family="stream-nisshi",
            status="inventory-only",
            files=["compose.evaluation.yaml", "compose.telemetry.nisshi.yaml"],
            replaces="evaluation:stream:default-broker",
        ),
        dict(
            family="storage-rustfs",
            status="inventory-only",
            files=["compose.storage.rustfs.yaml"],
            replaces="core:seaweedfs",
            requires=["isolated-project", "synthetic-same-commit-image"],
        ),
        *[
            dict(
                family=name,
                status="inventory-only",
                files=[f"examples/{name}/compose.yaml"],
                requires=["separate-comparison-project"],
            )
            for name in ("rill", "marimo")
        ],
        *[dict(family=mode, status="planned") for mode in plan.PLANNED],
    ]
    return [
        dict(status="renderable", **row) if "status" not in row else row
        for row in rows
    ]


def record_names(node):
    """YAML node内の変数名と構文分類だけを集める。値は保持しない。"""
    found = {}
    seen = set()
    pending = [node]
    while pending:
        item = pending.pop()
        if id(item) in seen:
            continue
        seen.add(id(item))
        if isinstance(item, ScalarNode):
            for match in ENV.finditer(item.value):
                name = match[1] or match[3]
                if name is not None:
                    found.setdefault(name, set()).add(ENV_KIND[match[2]])
        elif isinstance(item, SequenceNode):
            pending.extend(item.value)
        elif isinstance(item, MappingNode):
            pending.extend(child for pair in item.value for child in pair)
    return found


def source_mapping(node):
    """宣言の階層を読む。duplicateを無言で片方に決めない。"""
    if node is None:
        return {}
    if not isinstance(node, MappingNode):
        raise plan.PlanError("SOURCE_INVENTORY_UNAVAILABLE")
    result = {}
    for key, value in node.value:
        if not isinstance(key, ScalarNode) or key.value in result:
            raise plan.PlanError("SOURCE_INVENTORY_UNAVAILABLE")
        result[key.value] = value
    return result


def validate_layers(layer):
    """変数展開前に外部ファイルを取り込む構文を拒否する。"""
    if {"include", "<<"} & layer.keys():
        raise plan.PlanError("SOURCE_INVENTORY_UNAVAILABLE")
    pending = list(source_mapping(layer.get("services")).values())
    seen = set()
    while pending:
        node = pending.pop()
        if id(node) in seen:
            continue
        seen.add(id(node))
        service = source_mapping(node)
        if {
            "extends",
            "env_file",
            "label_file",
            "credential_spec",
        } & service.keys():
            raise plan.PlanError("SOURCE_INVENTORY_UNAVAILABLE")
        merge = service.get("<<")
        if isinstance(merge, SequenceNode):
            pending.extend(merge.value)
        elif merge is not None:
            pending.append(merge)


def source_layers(files):
    """許可済みレシピの追跡Composeをノードとして解析する。"""
    try:
        result = {}
        for filename in files:
            path = plan.ROOT / filename
            if path.is_symlink() or not path.is_file():
                raise plan.PlanError("SOURCE_INVENTORY_UNAVAILABLE")
            document = source_mapping(
                yaml.compose(path.read_text(encoding="utf-8"), yaml.SafeLoader)
            )
            validate_layers(document)
            result[filename] = document
        return result
    except (OSError, UnicodeError, yaml.YAMLError):
        raise plan.PlanError("SOURCE_INVENTORY_UNAVAILABLE") from None


def references(service):
    """展開済みサービスのconfigとsecretの論理参照を返す。"""
    result = {"configs": set(), "secrets": set()}
    for kind, entries in (
        ("configs", service.get("configs", [])),
        ("secrets", service.get("secrets", [])),
        ("secrets", (service.get("build") or {}).get("secrets", [])),
    ):
        for entry in entries:
            result[kind].add(
                entry if isinstance(entry, str) else entry["source"]
            )
    return result


def env_items(nodes):
    """同じ名前が複数の参照で使われる場合も分類を失わない。"""
    found = {}
    for node in nodes:
        for name, categories in record_names(node).items():
            found.setdefault(name, set()).update(categories)
    return {name: sorted(found[name]) for name in sorted(found)}


def service_sources(layers, name, service):
    """稼働サービスと参照資源に関係するソース宣言の上限集合を返す。"""
    source_files = []
    nodes = []
    refs = references(service)
    for filename, layer in layers.items():
        source = source_mapping(layer.get("services")).get(name)
        if source is not None:
            source_files.append(filename)
            nodes.append(source)
        for kind, keys in refs.items():
            blocks = source_mapping(layer.get(kind))
            nodes.extend(blocks[key] for key in keys if key in blocks)
    if not source_files:
        raise plan.PlanError("SOURCE_INVENTORY_UNAVAILABLE")
    return source_files, env_items(nodes)


def positive_decimal(value):
    """有限で正常な桁数・指数の数値だけを正確に比較する。"""
    if type(value) not in (int, float, str):
        raise ValueError
    number = Decimal(str(value))
    if not number.is_finite() or number <= 0:
        raise ValueError
    parts = number.as_tuple()
    if len(parts.digits) > 32 or not -32 <= parts.exponent <= 32:
        raise ValueError
    return number


def limit(service, key, deploy_key, scale, bound):
    """単一インスタンスの一致する明示上限だけを既知とする。"""
    unknown = dict(status="unknown", value=None)
    try:
        deploy = service.get("deploy", {})
        if not isinstance(deploy, dict):
            return unknown
        if deploy.get("mode", "replicated") != "replicated":
            return unknown
        instances = (deploy.get("replicas", 1), service.get("scale", 1))
        if any(type(value) is not int or value != 1 for value in instances):
            return unknown
        resources = deploy.get("resources", {})
        if not isinstance(resources, dict):
            return unknown
        additional = resources.get("limits", {})
        if not isinstance(additional, dict):
            return unknown
        if key not in service:
            return (
                unknown
                if deploy_key in additional
                else dict(status="unbounded", value=None)
            )
        number = positive_decimal(service[key])
        if deploy_key in additional:
            if number != positive_decimal(additional[deploy_key]):
                return unknown
        # 正確に変換してから上に丸め、わずかな上限差も既知扱いしない。
        with localcontext() as context:
            context.prec = 80
            scaled = number * scale
            if not 0 < scaled <= bound:
                return unknown
            value = int(scaled.to_integral_value(rounding=ROUND_CEILING))
        return dict(status="bounded", value=value)
    except (AttributeError, ValueError, DecimalException):
        return unknown


def summarize(model, layers):
    """限界を有効な各サービスに一度だけ数え、未設定は別集計。"""
    rows = []
    totals = {
        dimension: dict(known_total=0, unbounded=[], unknown=[])
        for dimension in DIMS
    }
    for name, service in sorted(model["services"].items()):
        source_files, environment = service_sources(layers, name, service)
        row = {
            "service": name,
            "compose_files": source_files,
            "profiles": sorted(service.get("profiles", [])),
            "environment_names": environment,
            "cpu_milli": limit(service, "cpus", "cpus", 1000, 10**9),
            "memory_mib": limit(
                service, "mem_limit", "memory", Decimal(1) / MIB, 10**9
            ),
        }
        rows.append(row)
        for dimension, subtotal in totals.items():
            metric = row[dimension]
            if metric["status"] == "bounded":
                subtotal["known_total"] += metric["value"]
            else:
                subtotal[metric["status"]].append(name)
    mode_environment = env_items(
        node for layer in layers.values() for node in layer.values()
    )
    return dict(
        services=rows, totals=totals, source_environment_names=mode_environment
    )


def synthetic_pair(value):
    """入力は単位付きの整数2個だけ。無効な文字列を出力しない。"""
    if PAIR.fullmatch(value) is None:
        raise ValueError
    return dict(zip(DIMS, map(int, value.split(":")), strict=True))


def budget(totals, capacity, reserve, prerequisites=()):
    """既知の不足と未評価事項を独立に示す。緩い値で許可しない。"""
    missing = [name for name in RESERVES if reserve.get(name) is None]
    gaps = {}
    for dimension, subtotal in totals.items():
        reserved = sum(
            pair[dimension] for pair in reserve.values() if pair is not None
        )
        remaining = capacity[dimension] - reserved
        known = subtotal["known_total"]
        gaps[dimension] = {
            "capacity": capacity[dimension],
            "declared_reserve": reserved,
            "remaining_after_declared_reserve": remaining,
            "known_minimum_shortfall": max(0, known - remaining),
            "known_minimum_capacity": known + reserved,
            "unknown_or_unbounded": bool(
                subtotal["unknown"] or subtotal["unbounded"]
            ),
        }
    has_shortfall = any(v["known_minimum_shortfall"] for v in gaps.values())
    unsettled = bool(missing or prerequisites) or any(
        v["unknown_or_unbounded"] for v in gaps.values()
    )
    return {
        "assessment": "deny"
        if has_shortfall
        else "indeterminate"
        if unsettled
        else "fits_static_envelope",
        "missing_reserves": missing,
        "reservations": {name: reserve.get(name) for name in RESERVES},
        "dimensions": gaps,
        "not_a_deployment_approval": True,
        "next_steps": (
            ["increase-capacity-or-compare-smaller-recipe"]
            if has_shortfall
            else []
        )
        + (["bound-or-measure-missing-items"] if unsettled else []),
    }


def alternatives(mode, options):
    """別見積の候補を返す。予算適合や機能同等性は保証しない。"""
    result = []
    if options["cdc"] or options["alerting"] or options["tools_metrics"]:
        result.append({"mode": mode, "options": [], "fit": "not_assessed"})
    if mode == "standard":
        result.append({"mode": "local-operations", "fit": "not_assessed"})
    if mode != "core-local":
        result.append({"mode": "core-local", "fit": "not_assessed"})
    return result


def main(argv=None):
    """起動はせず、データの照合結果だけを表示する。"""
    parser = plan.Parser(description=__doc__)
    parser.add_argument("mode", choices=(CATALOG, *MODES, LEGACY))
    parser.add_argument("--cdc", action="store_true")
    parser.add_argument("--alerting", action="store_true")
    parser.add_argument("--tools-metrics", action="store_true")
    parser.add_argument("--profile", choices=tuple(plan.EVALUATION))
    parser.add_argument("--synthetic-capacity", metavar="CPU_MILLI:MEMORY_MIB")
    for name in RESERVES:
        parser.add_argument(
            f"--reserve-{name.replace('_', '-')}",
            metavar="CPU_MILLI:MEMORY_MIB",
        )
    args = parser.parse_args(argv)
    mode = args.mode
    options = {
        key: getattr(args, key)
        for key in ("cdc", "alerting", "tools_metrics", "profile")
    }
    exit_status = 0
    report_status = "PASS"
    try:
        reserve = {name: getattr(args, f"reserve_{name}") for name in RESERVES}
        if mode in (CATALOG, "all-lab", *plan.PLANNED):
            if (
                any(options.values())
                or args.synthetic_capacity is not None
                or any(value is not None for value in reserve.values())
            ):
                raise plan.PlanError("INVALID_SELECTION")
            output = {"modes": MODES, "legacy_compatibility_mode": LEGACY}
            if mode in (CATALOG, "all-lab"):
                output["lab_choices"] = lab_choices()
            if mode != CATALOG:
                report_status = "NOT_IMPLEMENTED"
        else:
            limits = None
            if args.synthetic_capacity is not None:
                limits = synthetic_pair(args.synthetic_capacity)
                if not all(limits.values()):
                    raise plan.PlanError("INVALID_BUDGET")
                reserve = {
                    name: synthetic_pair(value) if value is not None else None
                    for name, value in reserve.items()
                }
            elif any(value is not None for value in reserve.values()):
                raise plan.PlanError("INVALID_BUDGET")
            files, profiles, expected = plan.recipe(mode, **options)
            layers = source_layers(files)
            model = plan.normalize(files, profiles)
            plan.report_model(model, expected)
            requirements = (
                ["distributed-runtime"]
                if args.profile == "distributed-check"
                else []
            )
            output = {
                "compose_files": files,
                "profiles": profiles,
                "runtime_prerequisites": requirements,
                **summarize(model, layers),
            }
            if limits is not None:
                output["budget"] = budget(
                    output["totals"], limits, reserve, requirements
                )
                assessment = output["budget"]["assessment"]
                if assessment != "fits_static_envelope":
                    output["budget"]["comparison_candidates"] = alternatives(
                        mode, options
                    )
                    exit_status = 3 if assessment == "deny" else 4
                    report_status = (
                        "INSUFFICIENT" if assessment == "deny" else "UNKNOWN"
                    )
        print(
            json.dumps(
                {
                    "mode": mode,
                    "scope": "synthetic-only",
                    "static_status": report_status,
                    "startup_verified": False,
                    **output,
                },
                sort_keys=True,
            )
        )
    except (
        plan.PlanError,
        ValueError,
        TypeError,
        KeyError,
        AttributeError,
    ) as error:
        reason = (
            str(error)
            if isinstance(error, plan.PlanError)
            else "INVALID_INPUT"
        )
        print(json.dumps({"error": reason}), file=sys.stderr)
        return 1
    return exit_status


if __name__ == "__main__":
    raise SystemExit(main())
