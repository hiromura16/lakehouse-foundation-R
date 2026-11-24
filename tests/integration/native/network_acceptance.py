"""任意アプリの実コンテナから、許可経路と遮断を同時に確認する。"""

import argparse
import json
import subprocess
from pathlib import Path

from acceptance import load_test_env

# 実際に起動している対象のIPすべてを試し、名前解決の失敗とは区別する。
PRIVATE = {
    "catalog": 8181,
    "catalog-db": 5432,
    "storage": 8333,
    "blackbox": 9115,
    "lakehouse": 8080,
}

# 通信は試験対象プロジェクト内の固定URLと、事前に取得したIPだけに限定する。
PROBE = r"""
const net = require('node:net');
const fs = require('node:fs');
const { privateEndpoints, reader } = JSON.parse(fs.readFileSync(0, 'utf8'));

function tcp(host, port) {
  // 同時に検査しても各接続は2秒で終了し、名前や資格情報を出力しない。
  return new Promise((resolve) => {
    const connection = net.createConnection({host, port});
    const finish = (reachable) => {
      connection.destroy();
      resolve(reachable);
    };
    connection.once('connect', () => finish(true));
    connection.once('error', () => finish(false));
    connection.setTimeout(2000, () => finish(false));
  });
}

async function response(path, options = {}) {
  // 自動redirectを止め、内部APIへ渡したトークンを別ホストへ送らない。
  const response = await fetch('http://lakehouse:8080' + path, {
    redirect: 'error',
    signal: AbortSignal.timeout(8000),
    ...options,
  });
  await response.body?.cancel();
  return response.status;
}

async function main() {
  const privateReachable = await Promise.all(
    privateEndpoints.map(({host, port}) => tcp(host, port))
  );
  console.log(JSON.stringify({
    privateReachable,
    automationDb: await tcp('tools-db', 5432),
    gateway: await response('/_gateway-health'),
    health: await response('/health'),
    unauthenticated: await response('/v1/data-products'),
    reader: await response('/v1/data-products', {
      headers: {Authorization: 'Bearer ' + reader},
    }),
    writer: await response('/run', {method: 'POST'}),
    arbitraryProxy: await response('/probe'),
  }));
}
main().catch(() => process.exitCode = 1);
"""


def validate(docker, compose, reader):
    """n8nの書込・管理系直通を拒否し、自身のDBと固定の読取経路を確認する。"""

    def output(*command, payload=None):
        """プロジェクトと読取トークンを、コマンド引数から分ける。"""
        return subprocess.run(
            command,
            input=payload,
            text=True,
            capture_output=True,
            check=True,
            timeout=30,
        ).stdout.strip()

    private = []
    for name, port in PRIVATE.items():
        identity = output(*compose, "ps", "-q", name)
        networks = json.loads(
            output(
                *docker,
                "inspect",
                "--format",
                "{{json .NetworkSettings.Networks}}",
                identity,
            )
        )
        hosts = [item["IPAddress"] for item in networks.values()]
        assert hosts and all(hosts), f"{name}の専用IPv4が確認できません。"
        private.extend({"host": host, "port": port} for host in hosts)
    actual = json.loads(
        output(
            *compose,
            "exec",
            "-T",
            "n8n",
            "node",
            "-e",
            PROBE,
            payload=json.dumps(
                {"privateEndpoints": private, "reader": reader}
            ),
        )
    )
    assert actual == {
        "privateReachable": [False] * len(private),
        "automationDb": True,
        "gateway": 204,
        "health": 200,
        "unauthenticated": 401,
        "reader": 200,
        "writer": 405,
        "arbitraryProxy": 404,
    }, actual
    print(
        f"n8nから専用IP宛{len(private)}経路の遮断と読取ゲートを確認しました。"
    )


def main():
    """すでに稼働する試験対象を指定し、アプリやDBの設定は変更しない。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", required=True)
    parser.add_argument("--site", type=Path, required=True)
    parser.add_argument("--tools", type=Path, required=True)
    parser.add_argument("--ipam", type=Path)
    parser.add_argument("--sudo", action="store_true")
    args = parser.parse_args()
    docker = ["sudo", "-n", "docker"] if args.sudo else ["docker"]
    compose = [
        *docker,
        "compose",
        "-p",
        args.project,
        "--env-file",
        str(args.site),
        "--env-file",
        str(args.tools),
        "-f",
        "compose.yaml",
        "-f",
        "compose.gui.yaml",
        "-f",
        "compose.tools.yaml",
    ]
    if args.ipam is not None:
        compose.extend(("-f", str(args.ipam)))
    validate(docker, compose, load_test_env(args.site)["READER_TOKEN"])


if __name__ == "__main__":
    main()
