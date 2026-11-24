"""Icebergの公開マニフェストを、OpenLineage形式の標準出力に変換する。"""

import argparse
import json
import re
import sys
from uuid import NAMESPACE_URL, uuid5

PRODUCER = "https://github.com/hiromura16/lakehouse-foundation-R"
SPEC = "https://openlineage.io/spec/2-0-2/OpenLineage.json"
VERSION = (
    "https://openlineage.io/spec/facets/1-0-0/DatasetVersionDatasetFacet.json"
)
CHANNELS = {"files": "", "cdc": "cdc_", "sql": "sql_"}
SITE_PATTERN = re.compile(r"[a-z][a-z0-9-]{0,62}")
RELEASE_PATTERN = re.compile(r"[0-9]{1,19}")


class SelectionError(ValueError):
    """利用者へ伝えてよい、設置先または公開世代の選択エラー。"""


def reference(namespace, table, snapshot):
    """基盤内の表名と、物理スナップショットの版を結びつける。"""
    return {
        "namespace": namespace,
        "name": table,
        "facets": {
            "version": {
                "_producer": PRODUCER,
                "_schemaURL": VERSION,
                "datasetVersion": str(snapshot),
            }
        },
    }


def events(release, *, site, channel):
    """公開を行ったジョブと、採用した派生表の版を出力する。

    設備側の旧版を保持した世代でも、公開ジョブの入力として記録する。
    物理表を生成したジョブと混同せず、公開マニフェストの生成だけを示す。
    """
    if not SITE_PATTERN.fullmatch(site) or channel not in CHANNELS:
        raise SelectionError("設置先の識別子または経路を確認してください。")
    prefix = CHANNELS[channel]
    namespace = f"iceberg://{site}/foundation-r"
    release_id = release["release_snapshot_id"]
    publication = reference(namespace, f"{prefix}gold._release", release_id)
    end_at = release["released_at"]
    if not (start_at := release.get("publication_started_at")):
        # 開始時刻が保存される前の世代では、表の版だけを通知する。
        return [
            {
                "eventTime": end_at,
                "producer": PRODUCER,
                "schemaURL": f"{SPEC}#/$defs/DatasetEvent",
                "dataset": publication,
            }
        ]

    job_name = f"{channel}_publish_manifest"
    run_id = uuid5(NAMESPACE_URL, f"{namespace}/{job_name}/{release_id}")
    common = {
        "producer": PRODUCER,
        "schemaURL": f"{SPEC}#/$defs/RunEvent",
        "job": {"namespace": namespace, "name": job_name},
        "run": {"runId": str(run_id)},
    }
    inputs = [
        reference(namespace, f"{prefix}{table}", snapshot)
        for table, snapshot in sorted(release["outputs"].items())
        if snapshot is not None
    ]
    return [
        {**common, "eventType": "START", "eventTime": start_at},
        {
            **common,
            "eventType": "COMPLETE",
            "eventTime": end_at,
            "inputs": inputs,
            "outputs": [publication],
        },
    ]


def select_release(core, channel, snapshot=None):
    """カタログから指定された公開世代だけを読む。"""
    from native import engine

    if snapshot is not None and not RELEASE_PATTERN.fullmatch(snapshot):
        raise SelectionError("公開世代IDを数字1〜19桁で指定してください。")
    prefix = CHANNELS[channel]
    catalog = core.catalog()
    release = engine.latest_release(
        core, catalog, prefix, int(snapshot) if snapshot is not None else None
    )
    if release is None:
        raise SelectionError("指定した経路に公開済みの世代が見つかりません。")
    return catalog, release


def main():
    """公開履歴を読み、レコード単位のJSONを標準出力へ出す。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--site", required=True, help="設置先の一意な識別子")
    parser.add_argument("--channel", choices=CHANNELS, default="files")
    parser.add_argument("--release", help="出力する公開世代ID。省略で最新")
    args = parser.parse_args()
    from native.api import core_module, report_error

    try:
        _, release = select_release(core_module(), args.channel, args.release)
        for event in events(release, site=args.site, channel=args.channel):
            print(json.dumps(event, ensure_ascii=False))
    except SelectionError as error:
        parser.error(str(error))
    except Exception as error:
        report_error(error, file=sys.stderr)
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
