"""隔離した閲覧構成で、内部サービスの停止と復旧の観測結果を検査する。"""

import argparse
from pathlib import Path

from acceptance import gui_authorization, wait_gui_probes

# Grafana・閲覧UI・閲覧ゲートウェイの監視経路はヘルスリレーに依存する。
UNAVAILABLE_BY_SERVICE = {
    "read-api": frozenset({"内部読取ゲートウェイ"}),
    "health-relay": frozenset(
        {
            "ヘルスリレー",
            "Grafana",
            "データ閲覧",
            "閲覧ゲートウェイ",
        }
    ),
}


def main():
    """起動や停止は試験側が行い、このCLIは監視結果を読み取る。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gui-env-file", required=True, type=Path)
    parser.add_argument("--prometheus-port", type=int, default=18505)
    parser.add_argument("--stopped-service", choices=UNAVAILABLE_BY_SERVICE)
    args = parser.parse_args()
    unavailable = UNAVAILABLE_BY_SERVICE.get(args.stopped_service, frozenset())
    wait_gui_probes(
        args.prometheus_port, gui_authorization(args.gui_env_file), unavailable
    )
    print(
        "停止中の監視結果を確認しました。"
        if unavailable
        else "全監視先の復旧を確認しました。"
    )


if __name__ == "__main__":
    main()
