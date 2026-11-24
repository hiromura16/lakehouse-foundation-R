"""選んだ公開世代の依存先を照合し、履歴保持の判断に使う。"""

import argparse
import json
import sys

from native.lineage import CHANNELS, SelectionError, select_release


def report(catalog, release, prefix):
    """復元・保持対象の表名と、参照時点のメタデータ状態を返す。"""
    expected = {
        **{
            f"bronze.{name}": value
            for name, value in release["inputs"].items()
        },
        **release["outputs"],
    }
    matches = []
    for local, snapshot in sorted(expected.items()):
        name = f"{prefix}{local}"
        table_exists = catalog.table_exists(name)
        if snapshot is None:
            available = table_exists
        else:
            available = table_exists and (
                catalog.load_table(name).snapshot_by_id(int(snapshot))
                is not None
            )
        matches.append(
            {
                "table": name,
                "snapshot_id": str(snapshot) if snapshot is not None else None,
                "metadata_available": available,
            }
        )
    return {
        "release_snapshot_id": release["release_snapshot_id"],
        "all_metadata_available": all(
            s["metadata_available"] for s in matches
        ),
        "references": matches,
    }


def main():
    """参照先を検査し、不足がある場合は終了コード1を返す。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--channel", choices=CHANNELS, default="files")
    parser.add_argument("--release", help="照合する公開世代ID。省略で最新")
    args = parser.parse_args()
    from native.api import core_module, report_error

    try:
        catalog, release = select_release(
            core_module(), args.channel, args.release
        )
        findings = report(catalog, release, CHANNELS[args.channel])
    except SelectionError as error:
        parser.error(str(error))
    except Exception as error:
        report_error(error, file=sys.stderr)
        return 1
    print(json.dumps(findings, ensure_ascii=False))
    return 0 if findings["all_metadata_available"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
